package it.uniroma2.faas.openwhisk.scheduler.util;

import it.uniroma2.faas.openwhisk.scheduler.Application;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * see@ https://thecodeexamples.com/maven/retrieve-version-maven-pom-xml-java/
 * see@ https://blog.soebes.de/blog/2014/01/02/version-information-into-your-appas-with-maven/
 */
public class ProjectSpecs {

    // note: required '/' to indicate absolute path
    // see {@link Class#getResourceAsStream}
    protected static final String PREFIX_JAR_ARCHIVE = "/META-INF/maven/it.uniroma2.faas/openwhisk-scheduler";
    protected static final String POM = "pom.xml";
    protected static final MavenXpp3Reader READER = new MavenXpp3Reader();
    protected final Model model;

    // Singleton with static initialization is thread-safe iff there is one
    //  JVM and one Class Loader (https://stackoverflow.com/questions/9615394/why-jvm-has-many-clasloaders-why-not-one)
    private static class SingletonContainer {
        private final static ProjectSpecs INSTANCE = new ProjectSpecs();
    }

    protected ProjectSpecs() {
        Model model = null;
        try {
            model = READER.read(Application.class.getResourceAsStream(String.format(
                    "%s/%s",
                    PREFIX_JAR_ARCHIVE,
                    POM
            )));
        } catch (XmlPullParserException | IOException e) {
            try {
                model = READER.read(new FileInputStream(POM));
            } catch (XmlPullParserException | IOException e2) {
                // ignore
            }
        }
        this.model = model;
    }

    public static ProjectSpecs getInstance() {
        return ProjectSpecs.SingletonContainer.INSTANCE;
    }

    public String getId() {
        return model == null
                ? null
                : model.getId();
    }

    public String getGroupId() {
        return model == null
                ? null
                : model.getGroupId();
    }

    public String getArtifactId() {
        return model == null
                ? null
                : model.getArtifactId();
    }

    public String getVersion() {
        return model == null
                ? null
                : model.getVersion();
    }

}