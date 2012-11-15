package HBaseIA.TwitBase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.joda.time.DateTime;

import utils.LoadUtils;
import HBaseIA.TwitBase.hbase.TwitsDAO;
import HBaseIA.TwitBase.hbase.UsersDAO;
import HBaseIA.TwitBase.model.User;

public class LoadTwits {

  public static final String usage =
    "loadtwits count\n" +
    "  help - print this message and exit.\n" +
    "  count - add count random twits to all TwitBase users.\n";

  private static String randTwit(List<String> words) {
    String twit = "";
    for (int i = 0; i < 12; i++) {
      twit += LoadUtils.randNth(words) + " ";
    }
    return twit;
  }

  private static DateTime randDT() {
    int year = 2010 + LoadUtils.randInt(5);
    int month = 1 + LoadUtils.randInt(12);
    int day = 1 + LoadUtils.randInt(28);
    return new DateTime(year, month, day, 0, 0, 0, 0);
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0 || "help".equals(args[0])) {
      System.out.println(usage);
      System.exit(0);
    }

    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);

    if (!admin.tableExists(UsersDAO.TABLE_NAME) ||
        !admin.tableExists(TwitsDAO.TABLE_NAME)) {
      System.out.println("Please use the InitTables utility to create " +
                         "destination tables first.");
      System.exit(0);
    }

    HTablePool pool = new HTablePool(conf, Integer.MAX_VALUE);
    UsersDAO users = new UsersDAO(pool);
    TwitsDAO twits = new TwitsDAO(pool);

    int count = Integer.parseInt(args[0]);
    List<String> words = LoadUtils.readResource(LoadUtils.WORDS_PATH);

    for(User u : users.getUsers()) {
      for (int i = 0; i < count; i++) {
        twits.postTwit(u.user, randDT(), randTwit(words));
      }
    }

    pool.closeTablePool(UsersDAO.TABLE_NAME);
  }

}
