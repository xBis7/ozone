package org.apache.hadoop.ozone.freon;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.codahale.metrics.Timer;
import picocli.CommandLine;

import static com.amazonaws.services.s3.internal.SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY;

/**
 * Class for setting up common setup aspects of S3BucketGenerator and
 * S3KeyGenerator
 * */
public class S3EntityGenerator extends BaseFreonGenerator {
    @CommandLine.Option(names = {"-e", "--endpoint"},
        description = "S3 HTTP endpoint",
        defaultValue = "http://localhost:9878")
    protected String endpoint;


    protected S3GeneratorSetupObjects commonSetup() {
        init();

        AmazonS3ClientBuilder amazonS3ClientBuilder =
            AmazonS3ClientBuilder.standard()
                .withCredentials(new EnvironmentVariableCredentialsProvider());

        if (endpoint.length() > 0) {
            amazonS3ClientBuilder
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(
                    new EndpointConfiguration(endpoint, "us-east-1"));
        } else {
            amazonS3ClientBuilder.withRegion(Regions.DEFAULT_REGION);
        }

        System.setProperty(DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY, "true");

        return new S3GeneratorSetupObjects(getMetrics().timer("key-create"),
            amazonS3ClientBuilder.build());
    }

    protected class S3GeneratorSetupObjects {
        private Timer timer;
        private AmazonS3 s3;

        public S3GeneratorSetupObjects(Timer timer, AmazonS3 s3) {
            this.timer = timer;
            this.s3 = s3;
        }

        public Timer getTimer() {
            return timer;
        }

        public void setTimer(Timer timer) {
            this.timer = timer;
        }

        public AmazonS3 getS3() {
            return s3;
        }

        public void setS3(AmazonS3 s3) {
            this.s3 = s3;
        }
    }

}
