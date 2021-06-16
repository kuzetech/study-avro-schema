package com.kuze.bigdata.domain;

public class OdpsTableRecord {

    private String id;
    private String name;
    private String app;
    private String dt;

    public OdpsTableRecord(String id, String name, String app, String dt) {
        this.id = id;
        this.name = name;
        this.app = app;
        this.dt = dt;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }
}
