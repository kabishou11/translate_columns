"""
数据库字段翻译脚本
- 配置通过 .env 管理
- 完全相同英文字段才复用缓存
- 低并发批量翻译，提高速度且保持稳定
- 实时落盘 translation_cache.json / failed_batches.json / progress.json
- 支持失败重试、断点续跑、失败补跑
- 支持 xlsx / xls / csv
- 支持按列名或 Excel 列字母配置读写列
"""
import json
import os
import re
import sys
import time
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import anthropic
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
BASE_URL = os.getenv("BASE_URL", "https://api.minimaxi.com/anthropic")
MODEL_NAME = os.getenv("MODEL_NAME", "MiniMax-M2.7")
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "4096"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "120"))
RETRY_TIMES = int(os.getenv("RETRY_TIMES", "3"))
RETRY_DELAY = float(os.getenv("RETRY_DELAY", "2"))
FINAL_RETRY_ROUNDS = int(os.getenv("FINAL_RETRY_ROUNDS", "1"))

INPUT_FILE = os.getenv("INPUT_FILE", "dcp_iol_iml表结构.xlsx")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "dcp_iol_iml表结构_翻译完成.xlsx")
SOURCE_COLUMN = os.getenv("SOURCE_COLUMN", "COLUMN_NAME")
TARGET_COLUMN = os.getenv("TARGET_COLUMN", "中文翻译")
TRANSLATION_CACHE = Path("translation_cache.json")
FAILED_BATCHES = Path("failed_batches.json")
PROGRESS_FILE = Path("progress.json")

client = anthropic.Anthropic(api_key=API_KEY, base_url=BASE_URL, timeout=REQUEST_TIMEOUT)
write_lock = threading.Lock()


class ProgressBar:
    def __init__(self, total, width=40):
        self.total = max(total, 1)
        self.width = width
        self.start_time = time.time()

    def render(self, completed, cache_hits, translated, failed, in_flight):
        percent = min(100.0, completed * 100.0 / self.total)
        filled = int(self.width * completed / self.total)
        bar = "█" * filled + "░" * (self.width - filled)
        elapsed = max(time.time() - self.start_time, 1e-6)
        speed = completed / elapsed
        sys.stdout.write(
            f"\r进度 |{bar}| {percent:5.1f}% [{completed}/{self.total}] "
            f"缓存复用:{cache_hits} 新翻译:{translated} 失败:{failed} 并发中:{in_flight} 速度:{speed:.1f}/s"
        )
        sys.stdout.flush()

    def newline(self):
        sys.stdout.write("\n")
        sys.stdout.flush()


def atomic_write_json(path: Path, data):
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    temp_path.replace(path)


def load_json_dict(path: Path):
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def load_json_list(path: Path):
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def normalize_column_token(value):
    return str(value or "").strip()


def excel_column_to_index(value):
    token = normalize_column_token(value).upper()
    if not token or not token.isalpha():
        return None

    index = 0
    for char in token:
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def resolve_column_name(df, column_config, create_if_missing=False):
    token = normalize_column_token(column_config)
    if not token:
        raise ValueError("列配置不能为空")

    if token in df.columns:
        return token

    column_index = excel_column_to_index(token)
    if column_index is not None:
        if column_index < len(df.columns):
            return df.columns[column_index]
        if create_if_missing:
            new_name = token
            if new_name not in df.columns:
                df[new_name] = pd.NA
            return new_name

    if create_if_missing:
        if token not in df.columns:
            df[token] = pd.NA
        return token

    raise ValueError(f"未找到列: {column_config}，当前列: {list(df.columns)}")


def read_input_file(file_path):
    suffix = Path(file_path).suffix.lower()
    if suffix == ".xlsx":
        return pd.read_excel(file_path)
    if suffix == ".xls":
        return pd.read_excel(file_path)
    if suffix == ".csv":
        return pd.read_csv(file_path)
    raise ValueError(f"不支持的文件格式: {suffix}，仅支持 .xlsx / .xls / .csv")


def write_output_file(df, file_path):
    suffix = Path(file_path).suffix.lower()
    if suffix == ".xlsx":
        df.to_excel(file_path, index=False, engine="openpyxl")
        return
    if suffix == ".xls":
        df.to_excel(file_path, index=False)
        return
    if suffix == ".csv":
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        return
    raise ValueError(f"不支持的文件格式: {suffix}，仅支持 .xlsx / .xls / .csv")


def extract_json_from_response(response):
    result_text = ""
    thinking_content = ""

    for block in response.content:
        if block.type == "text":
            result_text += block.text
        elif block.type == "thinking":
            thinking_content = getattr(block, "thinking", "") or ""

    if result_text:
        result_text = result_text.strip()
        result_text = re.sub(r"^```json\s*", "", result_text)
        result_text = re.sub(r"^```\s*", "", result_text)
        result_text = re.sub(r"\s*```$", "", result_text)
        return result_text

    if thinking_content:
        patterns = [
            r"\[[\s\S]*?\]",
            r"\{[\s\S]*?\}",
        ]
        for pattern in patterns:
            matches = re.findall(pattern, thinking_content)
            if matches:
                return max(matches, key=len)

    return ""


def convert_to_dict(data):
    if isinstance(data, dict):
        return OrderedDict((str(k), str(v)) for k, v in data.items() if k and v)

    if isinstance(data, list):
        result = OrderedDict()
        for item in data:
            if not isinstance(item, dict):
                continue
            field = item.get("field") or item.get("name") or item.get("column") or item.get("key")
            trans = item.get("translation") or item.get("chinese") or item.get("cn")
            if field and trans:
                result[str(field)] = str(trans)
        return result

    return OrderedDict()


def build_prompt(columns):
    cols_str = "\n".join(f"{i+1}. {col}" for i, col in enumerate(columns))
    return f"""你是一个专业的数据库字段翻译专家。请将以下英文数据库字段名翻译成简洁准确的中文。

要求：
1. 仅输出 JSON。
2. 输出格式必须是对象：{{\"字段名\": \"中文翻译\"}}。
3. key 必须与输入字段名完全一致。
4. value 只保留中文翻译，不要解释。
5. 不要输出数组，不要输出 description，不要输出 markdown。
6. 常见缩写：nbr→号码，amt→金额，dt→日期，tm→时间，id→标识/编号，cd→代码，desc→描述，txt→文本，flg→标志，num→编号，cnt→数量，bal→余额，typ→类型。
7. is_/has_/flag 类前缀优先翻译为“是否/标志”。

字段列表：
{cols_str}
"""


def translate_batch(columns, batch_id):
    prompt = build_prompt(columns)
    last_error = None

    for attempt in range(1, RETRY_TIMES + 1):
        try:
            response = client.messages.create(
                model=MODEL_NAME,
                max_tokens=MAX_TOKENS,
                system="你是一个专业的数据库字段翻译专家，只输出 JSON 对象。",
                messages=[
                    {"role": "user", "content": [{"type": "text", "text": prompt}]}
                ],
            )
            json_text = extract_json_from_response(response)
            if not json_text:
                raise ValueError("empty_response")

            raw_data = json.loads(json_text, object_pairs_hook=OrderedDict)
            translation = convert_to_dict(raw_data)
            filtered = OrderedDict((field, translation[field]) for field in columns if field in translation)
            if not filtered:
                raise ValueError("no_valid_translations")

            return {
                "batch_id": batch_id,
                "fields": columns,
                "translations": filtered,
                "error": None,
                "attempts": attempt,
            }
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            if attempt < RETRY_TIMES:
                time.sleep(RETRY_DELAY * attempt)

    return {
        "batch_id": batch_id,
        "fields": columns,
        "translations": OrderedDict(),
        "error": last_error,
        "attempts": RETRY_TIMES,
    }


def save_state(cache, failed_batches, progress):
    with write_lock:
        atomic_write_json(TRANSLATION_CACHE, cache)
        atomic_write_json(FAILED_BATCHES, failed_batches)
        atomic_write_json(PROGRESS_FILE, progress)


def chunk_list(items, size):
    return [items[i:i + size] for i in range(0, len(items), size)]


def process_round(batches, cache, progress_bar, stats, failed_batches_output, round_name):
    if not batches:
        return

    total_batches = len(batches)
    progress_bar.newline()
    print(f"开始{round_name}，批次数: {total_batches}，并发数: {MAX_WORKERS}", flush=True)

    in_flight = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {}
        for batch_id, fields in batches:
            print(f"[批次 {batch_id}] 提交 {len(fields)} 个字段", flush=True)
            future = executor.submit(translate_batch, fields, batch_id)
            future_map[future] = (batch_id, fields)
            in_flight += 1
            progress_bar.render(stats["completed"], stats["cache_hits"], stats["translated"], stats["failed"], in_flight)

        for future in as_completed(future_map):
            batch_id, fields = future_map[future]
            in_flight -= 1
            try:
                result = future.result()
            except Exception as e:
                result = {
                    "batch_id": batch_id,
                    "fields": fields,
                    "translations": OrderedDict(),
                    "error": f"FutureError: {e}",
                    "attempts": RETRY_TIMES,
                }

            translations = result["translations"]
            if translations:
                cache.update(translations)
                stats["translated"] += len(translations)
                stats["completed"] += len(translations)
                print(
                    f"\n[批次 {batch_id}] 完成: 新增 {len(translations)} 条，累计缓存 {len(cache)} 条，已写入 {TRANSLATION_CACHE}",
                    flush=True,
                )
            else:
                stats["failed"] += len(fields)
                failed_batches_output.append({
                    "batch_id": batch_id,
                    "fields": fields,
                    "error": result["error"],
                    "attempts": result["attempts"],
                })
                print(
                    f"\n[批次 {batch_id}] 失败: {len(fields)} 条，错误: {result['error']}",
                    flush=True,
                )

            progress = {
                "model": MODEL_NAME,
                "input_file": INPUT_FILE,
                "output_file": OUTPUT_FILE,
                "source_column": SOURCE_COLUMN,
                "target_column": TARGET_COLUMN,
                "batch_size": BATCH_SIZE,
                "max_workers": MAX_WORKERS,
                "completed": stats["completed"],
                "total": stats["total"],
                "cache_hits": stats["cache_hits"],
                "translated": stats["translated"],
                "failed": stats["failed"],
                "remaining": stats["total"] - stats["completed"],
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            save_state(cache, failed_batches_output, progress)
            progress_bar.render(stats["completed"], stats["cache_hits"], stats["translated"], stats["failed"], in_flight)


def main():
    print("=" * 60)
    print("数据库字段翻译工具")
    print("=" * 60)
    print(f"输入文件: {INPUT_FILE}")
    print(f"输出文件: {OUTPUT_FILE}")
    print(f"源字段列: {SOURCE_COLUMN}")
    print(f"写入列: {TARGET_COLUMN}")
    print(f"模型: {MODEL_NAME}")
    print(f"批次大小: {BATCH_SIZE}")
    print(f"并发数: {MAX_WORKERS}")
    print("=" * 60)

    df = read_input_file(INPUT_FILE)
    source_column_name = resolve_column_name(df, SOURCE_COLUMN)
    target_column_name = resolve_column_name(df, TARGET_COLUMN, create_if_missing=True)

    unique_cols = df[source_column_name].dropna().astype(str).unique().tolist()
    cache = load_json_dict(TRANSLATION_CACHE)

    cache_hits = sum(1 for field in unique_cols if field in cache)
    pending_cols = [field for field in unique_cols if field not in cache]

    print(f"总行数: {len(df):,}")
    print(f"源字段列解析为: {source_column_name}")
    print(f"写入列解析为: {target_column_name}")
    print(f"去重后字段数: {len(unique_cols):,}")
    print(f"缓存命中: {cache_hits:,}")
    print(f"待翻译: {len(pending_cols):,}")

    stats = {
        "total": len(unique_cols),
        "completed": cache_hits,
        "cache_hits": cache_hits,
        "translated": 0,
        "failed": 0,
    }

    failed_batches = []
    progress_bar = ProgressBar(len(unique_cols))
    progress_bar.render(stats["completed"], stats["cache_hits"], stats["translated"], stats["failed"], 0)

    initial_progress = {
        "model": MODEL_NAME,
        "input_file": INPUT_FILE,
        "output_file": OUTPUT_FILE,
        "source_column": SOURCE_COLUMN,
        "target_column": TARGET_COLUMN,
        "resolved_source_column": source_column_name,
        "resolved_target_column": target_column_name,
        "batch_size": BATCH_SIZE,
        "max_workers": MAX_WORKERS,
        "completed": stats["completed"],
        "total": stats["total"],
        "cache_hits": stats["cache_hits"],
        "translated": stats["translated"],
        "failed": stats["failed"],
        "remaining": stats["total"] - stats["completed"],
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    save_state(cache, failed_batches, initial_progress)

    batches = [(idx + 1, batch) for idx, batch in enumerate(chunk_list(pending_cols, BATCH_SIZE))]
    process_round(batches, cache, progress_bar, stats, failed_batches, "首轮翻译")

    for retry_round in range(1, FINAL_RETRY_ROUNDS + 1):
        if not failed_batches:
            break
        retry_fields = []
        for item in failed_batches:
            retry_fields.extend(item["fields"])
        failed_batches = []
        retry_batches = [
            (idx + 1, batch)
            for idx, batch in enumerate(chunk_list(retry_fields, BATCH_SIZE))
        ]
        process_round(retry_batches, cache, progress_bar, stats, failed_batches, f"失败补跑第 {retry_round} 轮")

    progress_bar.newline()
    print(f"最终缓存条数: {len(cache):,}")
    print(f"失败字段数: {sum(len(item['fields']) for item in failed_batches):,}")

    df[target_column_name] = df[source_column_name].astype(str).map(cache)
    filled = df[target_column_name].notna().sum()
    print(f"已填充: {filled:,}/{len(df):,} 行 ({100 * filled / len(df):.1f}%)")

    write_output_file(df, OUTPUT_FILE)
    print(f"结果已保存: {OUTPUT_FILE}")
    print(f"缓存文件: {TRANSLATION_CACHE}")
    print(f"失败文件: {FAILED_BATCHES}")
    print(f"进度文件: {PROGRESS_FILE}")


if __name__ == "__main__":
    main()
