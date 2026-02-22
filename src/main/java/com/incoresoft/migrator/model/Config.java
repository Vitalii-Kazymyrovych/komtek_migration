package com.incoresoft.migrator.model;

import java.util.List;

public class Config {
    public Postgres postgres;
    public List<String> dumpFiles;

    public static class Postgres {
        public String host;
        public int port;
        public String database;
        public String schema;
        public String user;
        public String password;
    }
}
