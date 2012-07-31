package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TablePreSplitter {

  Configuration conf;

  public static final String usage =
     "tablepresplit commands ...\n" +
     "  help - print this message and exit.\n" +
     "  create - create a new table with the provided start key, end key and number of splits.\n" +
     "     syntax: create <tableName> <familyName> <start key> <end key> <number of splits>\n"     ;

  public TablePreSplitter() {
    conf = HBaseConfiguration.create();
  }


  public boolean createPreSplitTable(String tableName, String family, String startKey, String endKey, int splits) throws IOException {
    List<String> l = new ArrayList<String>();
    l.add(family);
    return createPreSplitTable(tableName, l, startKey, endKey, splits);
  }

  public boolean createPreSplitTable(String tableName, List<String> families, String startKey, String endKey, int splits) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    byte[] table = Bytes.toBytes(tableName);
    byte[] start = Bytes.toBytes(startKey);
    byte[] end = Bytes.toBytes(endKey);
    HTableDescriptor desc = new HTableDescriptor(table);
    for(String f : families) {
      HColumnDescriptor col = new HColumnDescriptor(f);
      desc.addFamily(col);
    }
    admin.createTable(desc, start, end, splits);
    return admin.tableExists(table) && admin.getTableRegions(table).size()==splits;
  }

  public static void main(String[] args) {
    if (args.length == 0 || "help".equals(args[0])) {
      System.out.println(usage);
      System.exit(0);
    }

    if ("create".equals(args[0])) {
      TablePreSplitter mysplitter = new TablePreSplitter();
      String tableName = args[1];
      String familyName = args[2];
      String startKey = args[3];
      String endKey = args[4];
      int numOfSplits = Integer.parseInt(args[5]);
      try {
        boolean status = mysplitter.createPreSplitTable(tableName, familyName, startKey, endKey, numOfSplits);
        if(status)
          System.out.println("Table created successfully.");
        else
          System.out.println("Table creation failed.");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
