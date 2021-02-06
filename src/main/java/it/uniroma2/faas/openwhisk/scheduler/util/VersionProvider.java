package it.uniroma2.faas.openwhisk.scheduler.util;

import picocli.CommandLine;

public class VersionProvider implements CommandLine.IVersionProvider {

    @Override
    public String[] getVersion() {
        ProjectSpecs projectSpecs = ProjectSpecs.getInstance();
        return new String[] {String.format(
                "%s - v%s",
                projectSpecs.getArtifactId(),
                projectSpecs.getVersion()
        )};
    }

}