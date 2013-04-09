package HBaseIA.TwitBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import HBaseIA.TwitBase.hbase.RelationsDAO;
import HBaseIA.TwitBase.hbase.TwitsDAO;
import HBaseIA.TwitBase.hbase.UsersDAO;

public class InitTables {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);

    // first do no harm
    if (args.length > 0 && args[0].equalsIgnoreCase("-f")) {
      System.out.println("!!! dropping tables in...");
      for (int i = 5; i > 0; i--) {
        System.out.println(i);
        Thread.sleep(1000);
      }

      if (admin.tableExists(UsersDAO.TABLE_NAME)) {
        System.out.printf("Deleting %s\n", Bytes.toString(UsersDAO.TABLE_NAME));
        if (admin.isTableEnabled(UsersDAO.TABLE_NAME))
          admin.disableTable(UsersDAO.TABLE_NAME);
        admin.deleteTable(UsersDAO.TABLE_NAME);
      }

      if (admin.tableExists(TwitsDAO.TABLE_NAME)) {
        System.out.printf("Deleting %s\n", Bytes.toString(TwitsDAO.TABLE_NAME));
        if (admin.isTableEnabled(TwitsDAO.TABLE_NAME))
          admin.disableTable(TwitsDAO.TABLE_NAME);
        admin.deleteTable(TwitsDAO.TABLE_NAME);
      }

      if (admin.tableExists(RelationsDAO.FOLLOWS_TABLE_NAME)) {
        System.out.printf("Deleting %s\n", Bytes.toString(RelationsDAO.FOLLOWS_TABLE_NAME));
        if (admin.isTableEnabled(RelationsDAO.FOLLOWS_TABLE_NAME))
          admin.disableTable(RelationsDAO.FOLLOWS_TABLE_NAME);
        admin.deleteTable(RelationsDAO.FOLLOWS_TABLE_NAME);
      }

      if (admin.tableExists(RelationsDAO.FOLLOWED_TABLE_NAME)) {
          System.out.printf("Deleting %s\n", Bytes.toString(RelationsDAO.FOLLOWED_TABLE_NAME));
          if (admin.isTableEnabled(RelationsDAO.FOLLOWED_TABLE_NAME))
            admin.disableTable(RelationsDAO.FOLLOWED_TABLE_NAME);
          admin.deleteTable(RelationsDAO.FOLLOWED_TABLE_NAME);
        }
    }

    if (admin.tableExists(UsersDAO.TABLE_NAME)) {
      System.out.println("User table already exists.");
    } else {
      System.out.println("Creating User table...");
      HTableDescriptor desc = new HTableDescriptor(UsersDAO.TABLE_NAME);
      HColumnDescriptor c = new HColumnDescriptor(UsersDAO.INFO_FAM);
      desc.addFamily(c);
      admin.createTable(desc);
      System.out.println("User table created.");
    }

    if (admin.tableExists(TwitsDAO.TABLE_NAME)) {
      System.out.println("Twits table already exists.");
    } else {
      System.out.println("Creating Twits table...");
      HTableDescriptor desc = new HTableDescriptor(TwitsDAO.TABLE_NAME);
      HColumnDescriptor c = new HColumnDescriptor(TwitsDAO.TWITS_FAM);
      c.setMaxVersions(1);
      desc.addFamily(c);
      admin.createTable(desc);
      System.out.println("Twits table created.");
    }

    if (admin.tableExists(RelationsDAO.FOLLOWS_TABLE_NAME)) {
      System.out.println("Follows table already exists.");
    } else {
      System.out.println("Creating Follows table...");
      HTableDescriptor desc = new HTableDescriptor(RelationsDAO.FOLLOWS_TABLE_NAME);
      HColumnDescriptor c = new HColumnDescriptor(RelationsDAO.RELATION_FAM);
      c.setMaxVersions(1);
      desc.addFamily(c);
      admin.createTable(desc);
      System.out.println("Follows table created.");
    }

    if (admin.tableExists(RelationsDAO.FOLLOWED_TABLE_NAME)) {
        System.out.println("Followed table already exists.");
      } else {
        System.out.println("Creating Followed table...");
        HTableDescriptor desc = new HTableDescriptor(RelationsDAO.FOLLOWED_TABLE_NAME);
        HColumnDescriptor c = new HColumnDescriptor(RelationsDAO.RELATION_FAM);
        c.setMaxVersions(1);
        desc.addFamily(c);
        admin.createTable(desc);
        System.out.println("Followed table created.");
      }
  }
}
