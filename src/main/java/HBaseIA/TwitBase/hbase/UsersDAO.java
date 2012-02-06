package HBaseIA.TwitBase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import HBaseIA.TwitBase.Md5Utils;

public class UsersDAO {

	public static final byte[] TABLE_NAME = Bytes.toBytes("users");
	public static final byte[] INFO_FAM   = Bytes.toBytes("info");

	private static final byte[] USER_COL   = Bytes.toBytes("user");
	private static final byte[] NAME_COL   = Bytes.toBytes("name");
	private static final byte[] EMAIL_COL  = Bytes.toBytes("email");
	private static final byte[] PASS_COL   = Bytes.toBytes("password");
	private static final byte[] TWEETS_COL = Bytes.toBytes("tweet_count");

	private HTablePool pool;

	public UsersDAO(HTablePool pool) {
		this.pool = pool;
	}

	private static Put mkPut(User u) {
		Put p = new Put(Md5Utils.md5sum(u.user.toLowerCase()));
		p.add(INFO_FAM, USER_COL, Bytes.toBytes(u.user));
		p.add(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
		p.add(INFO_FAM, EMAIL_COL, Bytes.toBytes(u.email));
		p.add(INFO_FAM, PASS_COL, Bytes.toBytes(u.password));
		return p;
	}

	private static Get mkGet(String user) {
		Get g = new Get(Md5Utils.md5sum(user.toLowerCase()));
		g.addColumn(INFO_FAM, USER_COL);
		g.addColumn(INFO_FAM, NAME_COL);
		g.addColumn(INFO_FAM, EMAIL_COL);
		g.addColumn(INFO_FAM, PASS_COL);
		g.addColumn(INFO_FAM, TWEETS_COL);
		return g;
	}

	private static Scan mkScan() {
		Scan s = new Scan();
		s.addColumn(INFO_FAM, USER_COL);
		s.addColumn(INFO_FAM, NAME_COL);
		s.addColumn(INFO_FAM, EMAIL_COL);
		s.addColumn(INFO_FAM, PASS_COL);
		s.addColumn(INFO_FAM, TWEETS_COL);
		return s;
	}

	public void addUser(
			String user, String name, String email,
			String password) throws IOException {

		HTableInterface users = pool.getTable(UsersDAO.TABLE_NAME);

		Put p = mkPut(new User(user, name, email, password));
		users.put(p);

		pool.putTable(users);
	}

	public HBaseIA.TwitBase.model.User getUser(String user) throws IOException {
		HTableInterface users = pool.getTable(TABLE_NAME);

		Get g = mkGet(user);
		Result result = users.get(g);
		if (result.isEmpty())
			return null;

		User u = new User(result);
		pool.putTable(users);
		return u;
	}

	public List<HBaseIA.TwitBase.model.User> getUsers() throws IOException {
		HTableInterface users = pool.getTable(TABLE_NAME);

		ResultScanner results = users.getScanner(mkScan());
		ArrayList<HBaseIA.TwitBase.model.User> ret = new ArrayList<HBaseIA.TwitBase.model.User>();
		for(Result r : results) {
			ret.add(new User(r));
		}

		pool.putTable(users);
		return ret;
	}

	private static class User extends HBaseIA.TwitBase.model.User {
		public User(Result r) {
			this(
					r.getColumnLatest(INFO_FAM, USER_COL).getValue(),
					r.getColumnLatest(INFO_FAM, NAME_COL).getValue(),
					r.getColumnLatest(INFO_FAM, EMAIL_COL).getValue(),
					r.getColumnLatest(INFO_FAM, PASS_COL).getValue(),
					r.getColumnLatest(INFO_FAM, TWEETS_COL) == null ?
							Bytes.toBytes(0L) :
							r.getColumnLatest(INFO_FAM, TWEETS_COL).getValue());
		}

		public User(
				byte[] user, byte[] name, byte[] email,
				byte[] password, byte[] tweetCount) {
			this(
					Bytes.toString(user),
					Bytes.toString(name),
					Bytes.toString(email),
					Bytes.toString(password));
			this.tweetCount = Bytes.toLong(tweetCount);
		}

		public User(String user, String name, String email, String password) {
			this.user = user;
			this.name = name;
			this.email = email;
			this.password = password;
		}
	}
}
