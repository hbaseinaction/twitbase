package HBaseIA.TwitBase.cli;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTablePool;

import HBaseIA.TwitBase.hbase.UsersDAO;
import HBaseIA.TwitBase.model.User;

public class UserTool {

	public static final String usage =
			"usertool action ...\n" +
	        "  help - print this message and exit.\n" +
			"  add user name email password - add a new user.\n" +
			"  list - list installed users.\n";

	public static void main(String[] args) throws IOException {
		if (args.length == 0 || "help".equals(args[0])) {
			System.out.println(usage);
			System.exit(0);
		}

		HTablePool pool = new HTablePool();
		UsersDAO dao = new UsersDAO(pool);

		if ("add".equals(args[0])) {
			System.out.println("Adding user...");
			dao.addUser(args[1], args[2], args[3], args[4]);
			User u = dao.getUser(args[1]);
			System.out.println("Successfully added user " + u);

			pool.closeTablePool(UsersDAO.TABLE_NAME);
			System.exit(0);
		}

		if ("list".equals(args[0])) {
			for(User u : dao.getUsers()) {
				System.out.println(u);
			}

			pool.closeTablePool(UsersDAO.TABLE_NAME);
			System.exit(0);
		}
	}
}
