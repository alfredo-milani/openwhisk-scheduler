package it.uniroma2.faas.openwhisk.scheduler.data.source.domain.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Action {

    /*
    "action":{
          "name":"test_annotations",
          "path":"guest",
          "version":"0.0.1"
       },
     */

    private final String name;
    private final String path;
    private final String version;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Action(@JsonProperty("name") String name, @JsonProperty("path") String path,
                  @JsonProperty("version") String version) {
        this.name = name;
        this.path = path;
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "Action{" +
                "name='" + name + '\'' +
                ", path='" + path + '\'' +
                ", version='" + version + '\'' +
                '}';
    }

}