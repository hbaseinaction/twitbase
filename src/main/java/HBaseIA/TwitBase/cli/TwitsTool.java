package HBaseIA.TwitBase.cli;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTablePool;
import org.joda.time.DateTime;

import HBaseIA.TwitBase.hbase.TwitsDAO;
import HBaseIA.TwitBase.hbase.UsersDAO;
import HBaseIA.TwitBase.model.Twit;

public class TwitsTool {

	public static final String usage =
			"twitstool action ...\n" +
	        "  help - print this message and exit.\n" +
			"  post user text - post a new twit on user's behalf.\n" +
			"  list user - list all twits for the specified user.\n";

	public static void main(String[] args) throws IOException {
		if (args.length == 0 || "help".equals(args[0])) {
			System.out.println(usage);
			System.exit(0);
		}

		HTablePool pool = new HTablePool();
		TwitsDAO dao = new TwitsDAO(pool);

		if ("post".equals(args[0])) {
			System.out.println("Posting twit...");
			DateTime now = new DateTime();
			dao.postTwit(args[1], now, args[2]);
			Twit t = dao.getTwit(args[1], now);
			System.out.println("Successfully posted " + t);

			pool.closeTablePool(UsersDAO.TABLE_NAME);
			System.exit(0);
		}

		if ("list".equals(args[0])) {
			for(Twit t : dao.list(args[1])) {
				System.out.println(t);
			}

			pool.closeTablePool(UsersDAO.TABLE_NAME);
			System.exit(0);
		}
	}
}
