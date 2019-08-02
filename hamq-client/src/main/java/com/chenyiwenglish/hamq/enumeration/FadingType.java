package com.chenyiwenglish.hamq.enumeration;

public enum FadingType {

    NON(0, "不衰减"),
    LINEAR(1, "线性衰减"),
    SQUARE(2, "平方衰减"),
    CUBIC(3, "立方衰减"),
    EXPONENTIAL(4, "指数衰减");

    private Integer id;
    private String desc;

    private FadingType(Integer id, String dces) {
        this.id = id;
        this.desc = desc;
    }

    public Integer getId() {
        return id;
    }
}
