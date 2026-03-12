# Lighting Control System (灯光控制系统)

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.8%2B-brightgreen.svg)
![Framework](https://img.shields.io/badge/framework-PyQt5%20%2F%20PySide6-orange.svg)

这是一个基于 Python 开发的专业灯光控制系统，支持通过 **OPC UA/DA** 协议与工业自动化设备进行通信。系统集成了实时监控、分组控制、定时任务调度以及智能报警功能。

## 🌟 核心功能

- **多协议支持**：
  - **OPC UA**：采用 `asyncua` 实现异步全双工通信，支持证书认证与加密。
  - **OPC DA**：兼容传统工业自动化场景。
- **实时监控**：基于事件订阅机制，即时反馈点位状态变化，并自动转换为本地时区时间。
- **分组化管理**：支持对成百上千个点位进行灵活分组，实现“一键开关”批量控制。
- **智能策略调度**：
  - 内置基于 `asyncio` 的后台调度引擎。
  - 支持按时间点自动触发分组任务（如“上班模式”、“节电模式”）。
- **专业日志系统**：全流程操作审计与异常记录，保障系统长时间运行的稳定性。
- **现代 GUI 界面**：基于 PyQt5 设计的直观仪表盘与配置后台。

## 🏗️ 项目架构

```text
lightingcontrol/
├── core/               # 核心逻辑：计划任务、数据持久化管理
├── opc/                # 通信引擎：OPC UA/DA 客户端实现
├── ui/                 # 界面组件：PyQt 窗口与自定义控件
├── data/               # 配置存储
├── utils/              # 通用工具：日志记录、时间转换
└── main.py             # 入口程序
```

## 🚀 快速开始

### 1. 环境准备
确保已安装 Python 3.8 或更高版本。

### 2. 安装依赖
```bash
pip install -r requirements.txt
```
*(注：主要依赖包括 `asyncua`, `PyQt5`, `qasync`, `cryptography` 等)*

### 3. 配置连接
- 本系统首次连接 OPC UA 服务器时会自动生成客户端证书 (`client_cert.der`)。
- 请确保将服务器证书放置在根目录下（或根据提示导入）。

### 4. 运行
```bash
python main.py
```

## 🛠️ 技术选型

- **语言**: Python 3.x
- **GUI 框架**: PyQt5 / PySide6 (支持跨平台)
- **异步框架**: `asyncio` + `qasync` (完美结合 Qt 事件循环与异步 IO)
- **安全**: `cryptography` (X.509 证书生成与解析)

## 📄 开源协议

本项目采用 [MIT License](LICENSE) 开源协议。
