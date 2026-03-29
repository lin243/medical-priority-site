# 医药资讯优先级系统

这是一个可直接部署到 GitHub Pages 的纯静态站点目录。

## 目录说明

- `index.html`：网站入口
- `styles.css`：样式文件
- `app.js`：页面逻辑
- `medical_mock_data.js`：医药资讯数据
- `aacr_embedded_data.js`：AACR 数据

## 部署方式

将本目录全部文件上传到 GitHub 仓库根目录，然后在 GitHub Pages 中选择从 `main` 分支的 `/ (root)` 发布即可。

## 纯静态说明

- 当前版本不依赖外部 CDN
- 页面字体使用本地字体栈
- 导出默认使用 `CSV`
- 导入支持 `CSV`
- 如果页面额外注入 `SheetJS/XLSX`，也可继续导入和导出 Excel

## 本地预览

```powershell
cd C:\Users\YYMF\Desktop\medical-priority-site
python -m http.server 8000
```

## NPNews 脚本默认运行方式

涉及 ES 和大模型调用时，默认使用同目录下的 PowerShell 入口，而不是直接运行 `.py`：

```powershell
cd C:\Users\YYMF\Desktop\medical-priority-site
.\export_npnews_daily_table.ps1
.\score_npnews_daily_table.ps1 --input .\npnews_daily_table_2026-03-26.csv
.\run_npnews_pipeline.ps1 --date 2026-03-26
```

这些入口会自动使用 `uv run --project E:\llm-master` 启动，避免系统 Python 缺依赖。
