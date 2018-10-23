package com.kafka.consumer.enums;

import com.google.gson.annotations.SerializedName;

public enum EventType {
    @SerializedName("login") LOGIN,
    @SerializedName("build") BUILD,
    @SerializedName("attack") ATTACK,
    @SerializedName("other") OTHER
}
