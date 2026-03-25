# 2026-03-25 公开 GitHub 发布设计

## 目标
在不影响当前正在运行任务的前提下，将当前数据库字段翻译脚本项目发布到公开 GitHub 仓库，并确保敏感配置、本地数据文件和运行产物不被上传。

## 发布范围
### 保留到仓库中的文件
- `translate_columns.py`
- `README.md`
- `requirements.txt`
- `.env.example`
- `.gitignore`
- `docs/plans/2026-03-25-github-publish-design.md`

### 排除出仓库的文件
- `.env`
- 所有本地 Excel / CSV 数据文件
- `translation_cache.json`
- `failed_batches.json`
- `progress.json`
- `.venv/`
- `.claude/`

## 实施方案
1. 新建 `.env.example`，仅保留配置项与占位值，不包含真实密钥。
2. 新建或补充 `.gitignore`，统一忽略敏感文件、数据文件和运行状态文件。
3. 视情况更新 `README.md`，说明从 `.env.example` 复制生成 `.env` 的使用方式。
4. 初始化本地 git 仓库。
5. 仅暂存安全文件，避免误提交当前数据与运行产物。
6. 创建提交并关联远程仓库 `https://github.com/kabishou11/translate_columns.git`。
7. 推送到公开仓库。

## 不影响当前任务的约束
- 不修改已有 `.env`
- 不移动、不删除当前数据文件
- 不修改 `translation_cache.json`、`failed_batches.json`、`progress.json`
- 只新增安全文件和 Git 元数据文件
- Git 操作只读取现有文件并提交允许范围内的文件

## 风险控制
- 通过 `.gitignore` 阻止敏感文件和运行产物进入版本库。
- 通过 `.env.example` 替代真实 `.env`，避免 API key 泄露。
- 公开仓库仅保留可复用代码与文档，不暴露业务数据。

## 验证标准
- `git status` 中不会出现 `.env`、数据文件和运行状态文件被暂存。
- 远程仓库中可见的仅为代码、文档、示例配置与 Git 忽略规则。
- 当前本地运行任务文件保持原位且未被覆盖。
