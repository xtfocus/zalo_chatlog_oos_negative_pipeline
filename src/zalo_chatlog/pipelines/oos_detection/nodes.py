import numpy as np
import torch
from pandas import DataFrame


def oos_feature_extraction(chatlog: DataFrame) -> DataFrame:
    """
    Extract features needed for out-of-stock detection
    """

    chatlog = chatlog[
        (chatlog["sender"] == "Nhân viên") & (chatlog["event"] == "agent_text_other")
    ].reset_index(drop=True)

    chatlog["readable_event"] = chatlog["readable_event"].str.lower()

    # Remove likely irrelevant texts
    chatlog = chatlog[
        ~chatlog["readable_event"].str.contains(
            "chào|xin phép|đợi em|chờ em|cảm ơn|cám ơn|người đặt"
        )
    ].reset_index()

    chatlog = chatlog.groupby("daily_session_code")["readable_event"].apply(list)
    chatlog = DataFrame(chatlog).reset_index()
    chatlog["features"] = chatlog["readable_event"].apply(lambda x: "\n".join(x))

    return chatlog[["daily_session_code", "features"]]
