package com.chenyiwenglish.hamq.enumeration;

public enum Status {

    RUNNING(0, "运行中"),
    PAUSE(1, "暂停");

    private Integer id;
    private String desc;

    private Status(Integer id, String dces) {
        this.id = id;
        this.desc = desc;
    }

    public Integer getId() {
        return id;
    }
}
