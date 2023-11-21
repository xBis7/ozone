package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.fs.FileSystem;

public class OFSDelegationTokenTest {

  public static void main(String[] args) {

    Configuration conf = new Configuration();
    conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
    conf.set("fs.defaultFS", "ofs://omservice");

    UserGroupInformation.setConfiguration(conf);

    try {
      // login (with kinit to keep it simple)
      UserGroupInformation.loginUserFromSubject(null);
      // get the current UGI
      UserGroupInformation currentUGI = UserGroupInformation.getCurrentUser();
      // no tokens
      System.out.println("current UGI tokens (should be empty): " + currentUGI.getCredentials().getAllTokens());

      // fetch a delegation token from Ozone
      FileSystem fs = FileSystem.get(conf);
      Token<?> token = fs.getDelegationToken("abc");
      // add the token to the current UGI
      currentUGI.addToken(token);
      // we should have a token
      System.out.println("current UGI tokens (we requested one, should be populated in current subject): " + currentUGI.getCredentials().getAllTokens());

    } catch (Exception e){
      e.printStackTrace();
    }
  }

}
