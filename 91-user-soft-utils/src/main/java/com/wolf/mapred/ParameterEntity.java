package com.wolf.mapred;

/**
 *
 * @author aladdin
 */
public final class ParameterEntity {

    private final String name;
    private String value = "";
    private final String describe;

    public ParameterEntity(String name, String describe) {
        this.name = name;
        this.describe = describe;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDescribe() {
        return describe;
    }
}
