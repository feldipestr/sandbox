from pydantic import BaseModel
from typing import Any


class RequestResult(BaseModel):
    status_code: int = 0
    response_json: Any = None
    err_message: str = ''


class PipelineResult(BaseModel):
    result_bool: bool = False
    result_int: int = 0
    result_float: float = 0.0
    result_dict: dict = {}
    result_list: list = []
    result_str: str = ''
    result_any: Any = None
    output_paths: dict = {}
