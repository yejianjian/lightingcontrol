# -*- coding: utf-8 -*-
"""OPC DA（COM/DCOM）客户端包。

对外只暴露 opcda.client 的稳定接口；上层引擎（opcda.engine）与 UI 应只依赖这里，
不要直接使用 opcda.com 中的 COM 细节。
"""

from . import com
from .client import (
    DaItem,
    DaValue,
    OpcDaClient,
    OpcDaError,
    list_servers,
    pump_events,
    vtype_to_type_name,
)

__all__ = [
    'DaItem',
    'DaValue',
    'OpcDaClient',
    'OpcDaError',
    'list_servers',
    'pump_events',
    'vtype_to_type_name',
    'com',
]
