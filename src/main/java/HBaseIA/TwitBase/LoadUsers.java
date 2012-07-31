package HBaseIA.TwitBase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTablePool;

import utils.LoadUtils;

import HBaseIA.TwitBase.hbase.UsersDAO;

public class LoadUsers {

  public static final String usage =
    "loadusers count\n" +
    "  help - print this message and exit.\n" +
    "  count - add count random TwitBase users.\n";

  private static String randName(List<String> names) {
    String name = LoadUtils.randNth(names) + " ";
    name += LoadUtils.randNth(names);
    return name;
  }

  private static String randUser(String name) {
    return String.format("%s%2d", name.substring(5), LoadUtils.randInt(100));
  }

  private static String randEmail(String user, List<String> words) {
    return String.format("%s@%s.com", user, LoadUtils.randNth(words));
  }

  public static void main(String[] args) throws IOException {
    if (args.length == 0 || "help".equals(args[0])) {
      System.out.println(usage);
      System.exit(0);
    }

    HTablePool pool = new HTablePool();
    UsersDAO dao = new UsersDAO(pool);

    int count = Integer.parseInt(args[0]);
    List<String> names = LoadUtils.readResource(LoadUtils.NAMES_PATH);
    List<String> words = LoadUtils.readResource(LoadUtils.WORDS_PATH);

    for (int i = 0; i < count; i++) {
      String name = randName(names);
      String user = randUser(name);
      String email = randEmail(user, words);
      dao.addUser(user, name, email, "abc123");
    }

    pool.closeTablePool(UsersDAO.TABLE_NAME);
  }
}
