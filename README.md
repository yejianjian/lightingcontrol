# Lighting Control System (灯光控制系统) V1.1.0

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.8%2B-brightgreen.svg)
![Framework](https://img.shields.io/badge/framework-PyQt5%20%2F%20PySide6-orange.svg)

这是一个基于 Python 开发的专业灯光控制系统，支持通过 **OPC UA/DA** 协议与工业自动化设备进行通信。系统集成了实时监控、分组控制、定时任务调度以及智能报警功能。

## 🌟 核心功能与技术亮点

- **高性能 OPC UA 通信**：
  - 采用 `asyncua` 实现异步全双工通信，支持证书认证与加密。
  - **稳定性增强**：针对 24/7 长时间运行优化了资源清理与异步心跳监控逻辑。
- **全类型 Node ID 支持**：
  - 完美支持 OPC UA 标准的所有标识符类型：数字 (`i=`)、字符串 (`s=`)、GUID (`g=`) 和 不透明型 (`b=`)。
- **智能分组与流控**：
  - 支持对海量点位进行灵活分组。
  - **并发控制系统**：内置 `asyncio.Semaphore` 机制，在大规模批量操作时自动平衡网络负载，防止服务器冲击。
- **策略化调度引擎**：
  - 基于 `asyncio` 的后台调度系统，支持按时间点及其组合（如周一至周五）自动触发。
- **Excel 深度集成**：
  - 支持别名表与分组逻辑的一键快速导入/导出，具备智能格式兼容处理。

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
