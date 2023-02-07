package com.myorg;

import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;

import java.util.Arrays;

public class CdkAwsGlueDeploymentApp {
    public static void main(final String[] args) throws Exception {
        App app = new App();

        new CdkAwsGlueDeploymentStack(app, "CdkAwsGlueDeploymentStack", StackProps.builder().build());

        app.synth();
    }
}

