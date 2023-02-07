package com.myorg;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.IamException;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;


import java.net.URL;
import java.util.ArrayList;
import java.util.List;


public class CdkAwsGlueDeploymentStack extends Stack {

    public static final String ROLE_NANE="iam-role-glue-demo1";

    public static final String BUCKET_NAME="aws-glue-practice-raviteja";

    public static final String KEY_NAME_SRC="data-store";

    public static final String KEY_NAME_RST="target-data-store";

    public static final String DATABASE_NAME="newDatabase";

    public static final String CRAWLER_NAME="new_crawler_demo";
    public static final String assumeRolePolicyDocument= "{\n" +
            "        \"Version\": \"2012-10-17\",\n" +
            "                \"Statement\": [\n" +
            "        {\n" +
            "            \"Effect\": \"Allow\",\n" +
            "                \"Principal\": {\n" +
            "            \"Service\": [\n" +
            "            \"ec2.amazonaws.com\"\n" +
            "                                ]\n" +
            "        },\n" +
            "            \"Action\": [\n" +
            "            \"sts:AssumeRole\"\n" +
            "                            ]\n" +
            "        }\n" +
            "                    ]\n" +
            "    }";

    public CdkAwsGlueDeploymentStack(final Construct scope, final String id) throws Exception {
        this(scope, id, null);
    }

    public CdkAwsGlueDeploymentStack(final Construct scope, final String id, final StackProps props) throws Exception {
        super(scope, id, props);
    }


    Region region= Region.EU_WEST_1;
    IamClient iam = IamClient.builder()
            .region(region)
            .build();

    String iamRole= createIAMRole(iam,ROLE_NANE);

    S3Client s3Client = S3Client.builder().region(region).build();

    URL urlSourceFolder = s3Client.utilities().getUrl(GetUrlRequest.builder()
            .bucket(BUCKET_NAME)
            .key(KEY_NAME_SRC)
            .build());
    AWSGlueClient glueClient=(AWSGlueClient)AWSGlueClient.builder().withRegion(Regions.EU_WEST_1).build();



     String name= createDataBase(glueClient,DATABASE_NAME,urlSourceFolder.toURI().toString());

  //  String newCrawler= createGlueCrawler(glueClient, iamRole, urlSourceFolder.toURI().toString(),DATABASE_NAME,CRAWLER_NAME);


    public static String createIAMRole(IamClient iam, String rolename) throws Exception {

        try {

            CreateRoleRequest request = CreateRoleRequest.builder()
                    .roleName(rolename)
                    .assumeRolePolicyDocument(assumeRolePolicyDocument)
                    .description("Created using the AWS SDK for Java")
                    .build();

            CreateRoleResponse response = iam.createRole(request);
            System.out.println("The ARN of the role is "+response.role().arn());

        } catch (IamException e) {

            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }


    public static String createDataBase(AWSGlueClient glueClient,String dbname,String locationUri) {

        try {

            DatabaseInput input = new DatabaseInput().withDescription("built with cdk").withName(dbname);

            CreateDatabaseRequest request = new CreateDatabaseRequest().withDatabaseInput(input);
            glueClient.createDatabase(request);

        } catch (AWSGlueException  e) {
          // Logger.info("error creating database");
            System.exit(1);
        }
        return "";
    }
    public static String createGlueCrawler(AWSGlueClient glueClient,
                                         String iam,
                                         String s3Path,
                                         String dbName,
                                         String crawlerName) {

        try {
            S3Target s3Target = S3Target.builder()
                    .path(s3Path)
                    .build();


            // Add the S3Target to a list.
            List<S3Target> targetList = new ArrayList<>();
            targetList.add(s3Target);

            CrawlerTargets targets = CrawlerTargets.builder()
                    .s3Targets(targetList)
                    .build();

            CreateCrawlerRequest crawlerRequest = CreateCrawlerRequest.builder()
                    .databaseName(dbName)
                    .name(crawlerName)
                    .description("Created by the AWS Glue Java API")
                    .targets(targets)
                    .role(iam)
                    .build();

            glueClient.createCrawler(crawlerRequest);
            System.out.println(crawlerName +" was successfully created");

        } catch (AWSGlueException e) {
          //  System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }



}
