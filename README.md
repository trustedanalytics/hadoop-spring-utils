# Utils to initialize Hadoop core Java objects

For now it only supports HDFS and so documentation is only limited to this case. Will be extended as new features are added.

## Usage

In your spring configuration class add annotation

```
  import org.trustedanalytics.utils.hdfs.EnableHdfs;

  @Configuration
  @EnableHdfs
  public class SomeConfigClass {
  }
```

Now you can use in some class:

```
  ...
  
  @Autowired
  private HdfsConfig hdfsConfig;

  public void someMethod() {
    FileSystem fs = hdfsConfig.getFileSystem();
    ...
  }
  
  ...
```

Which FileSystem is created depends on Spring profile that is active (to set profile, set SPRING_PROFILES_ACTIVE, for example: `export SPRING_PROFILES_ACTIVE=local`)

### cloud
This is profile that is usually activated in Cloud Foundry. It reads:

- credentials for authentication/identity from user provided service ("ups") bound to application 
(i.e. kerberos-service). If "ups" does not exists in your space, you can create it with commands:

In environment with kerberos
```
  cf cups kerberos-service -p '{ "kdc": "kdc-host", "kpassword": "kerberos-password", "krealm": "kerberos-realm", "kuser": "kerberos-user" }'
  cf bs <app_name> kerberos-service
```

In environment with simple authentication method set
```
  cf cups identity-service -p '{ "kuser": "user-name" }'
  cf bs <app_name> identity-service
```

- other parameters that are provided by binding to HDFS service instance


### local

It works on a local file system. If the env variable **FOLDER** is set than this is the one that is going to be used. Otherwise, temporary folder will be created that will be removed on normal application termination.
