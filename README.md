# Utils to initialize Hadoop core Java objects

For now it only supports HDFS and so documentation is only limited to this case. Will be extended as new features are added.

## Usage

In your spring configuration class add annotation

```
  @Configuration
  @ComponentScan("org.trustedanalytics.utils.hdfs")
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

Which FileSystem is created depends on Spring profile that is active

### cloud
This is profile that is usually activated in Cloud Foundry. It reads:

- property **hdfs.user**
- other parameters that are provided by binding to HDFS service instance


### secure, kerberos, krb
It setups Kerberos and reads related parameters. Other than that it does the same initialization as in *cloud* profile.

Paramteres related to Kerberos:

- **kerberos.user**
- **kerberos.password**
- other parameters that are provided by binding to HDFS service instance

Note that those in spring can be transleted also to environment variables like **KERBEROS_KDC**


### hdfs-local

It just reads two environment variables:

- **HDFS_URI**
- **HDFS_USER**


### default, localfs

It works on a local file system. If the env variable **FOLDER** is set than this is the one that is going to be used. Otherwise, temporary folder will be created that will be removed on normal application termination.

