# 数据库字段翻译工具

翻译数据库的字段为中文名，提高数据分类分级的准确率。

## 功能说明

该脚本用于批量读取表结构文件中的英文字段名，调用大模型翻译为中文，并将翻译结果写回目标列。

当前支持：
- `.xlsx`
- `.xls`
- `.csv`

并保留以下能力：
- `.env` 统一管理模型与文件配置
- 完全相同英文字段才复用缓存
- 低并发批量翻译
- 实时落盘缓存、失败批次、进度信息
- 失败重试与失败补跑
- 可按列名或 Excel 列字母指定源列和目标列

---

## 文件说明

- `translate_columns.py`：主脚本
- `.env.example`：配置示例文件，复制后改名为 `.env`
- `.env`：本地实际配置文件，不应提交到仓库
- `translation_cache.json`：翻译缓存
- `failed_batches.json`：失败批次记录
- `progress.json`：当前进度信息

---

## 环境要求

- Python 3.13
- 建议使用 `uv`

安装依赖示例：

```bash
uv venv .venv --python 3.13
uv pip install anthropic pandas python-dotenv openpyxl
```

如果需要处理 `.xls`，还需要根据本地环境补充对应 Excel 引擎依赖。

---

## .env 配置

先复制示例配置：

```bash
cp .env.example .env
```

再按实际环境修改 `.env`：

```env
# API配置
ANTHROPIC_API_KEY=your_key
BASE_URL=https://api.minimaxi.com/anthropic

# 文件与列配置
INPUT_FILE=dcp_iol_iml表结构.xlsx
OUTPUT_FILE=dcp_iol_iml表结构_翻译完成.xlsx
SOURCE_COLUMN=COLUMN_NAME
TARGET_COLUMN=中文翻译

# 模型配置
MODEL_NAME=MiniMax-M2.7
MAX_TOKENS=4096

# 批处理配置
BATCH_SIZE=20
MAX_WORKERS=3
REQUEST_TIMEOUT=120
RETRY_TIMES=3
RETRY_DELAY=2
FINAL_RETRY_ROUNDS=1
```

---

## 列配置方式

### 1. 使用列名

```env
SOURCE_COLUMN=COLUMN_NAME
TARGET_COLUMN=中文翻译
```

### 2. 使用 Excel 列字母

```env
SOURCE_COLUMN=F
TARGET_COLUMN=I
```

说明：
- 源列支持列名或列字母
- 目标列支持列名或列字母
- 如果目标列不存在，脚本会自动创建该列

---

## 运行方式

```bash
python translate_columns.py
```

---

## 运行过程中的输出

脚本会输出：
- 当前输入文件、输出文件、源列、目标列
- 去重后字段数
- 缓存命中数
- 待翻译数
- 批次提交与完成情况
- 进度条、并发数、失败数、速度

同时会持续写入：
- `translation_cache.json`
- `failed_batches.json`
- `progress.json`

---

## 缓存复用规则

仅当英文字段名**完全一致**时，才会复用已有翻译。

例如：
- `user_name` 和 `user_name`：可复用
- `user_name` 和 `userName`：不可复用
- `amt` 和 `amount`：不可复用

---

## 输出规则

- 翻译结果写入 `TARGET_COLUMN` 指定列
- `.csv` 输出使用 `utf-8-sig` 编码，便于 Excel 打开
- `.xlsx` 输出使用 `openpyxl`

---

## 适合后续切表的修改点

后续如果切换到新表，通常只需要改 `.env` 中这几项：

```env
INPUT_FILE=
OUTPUT_FILE=
SOURCE_COLUMN=
TARGET_COLUMN=
```

其余并发、重试、模型参数可按需要继续调整。
