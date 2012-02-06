package HBaseIA.TwitBase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;

import HBaseIA.TwitBase.Md5Utils;

public class TwitsDAO {

	public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
	public static final byte[] TWITS_FAM  = Bytes.toBytes("t");

	private static final byte[] USER_COL   = Bytes.toBytes("user");
	private static final byte[] TWIT_COL   = Bytes.toBytes("text");
	private static final int longLength = 8; // bytes

	private HTablePool pool;

	public TwitsDAO(HTablePool pool) {
		this.pool = pool;
	}

	private static byte[] mkRowKey(Twit t) {
		return mkRowKey(t.user, t.dt);
	}

	private static byte[] mkRowKey(String user, DateTime dt) {
		byte[] userHash = Md5Utils.md5sum(user);
		byte[] timestamp = Bytes.toBytes(-1 * dt.getMillis());
		byte[] rowKey = new byte[Md5Utils.MD5_LENGTH + longLength];
		System.arraycopy(userHash, 0, rowKey, 0, Md5Utils.MD5_LENGTH);
		System.arraycopy(timestamp, 0, rowKey, Md5Utils.MD5_LENGTH+1, longLength);
		return rowKey;
	}

	private static Put mkPut(Twit t) {
		Put p = new Put(mkRowKey(t));
		p.add(TWITS_FAM, USER_COL, Bytes.toBytes(t.user));
		p.add(TWITS_FAM, TWIT_COL, Bytes.toBytes(t.text));
		return p;
	}

	private static Get mkGet(String user, DateTime dt) {
		Get g = new Get(mkRowKey(user, dt));
		g.addColumn(TWITS_FAM, USER_COL);
		g.addColumn(TWITS_FAM, TWIT_COL);
		return g;
	}

//	private static String to_str(byte[] xs) {
//		StringBuilder sb = new StringBuilder(xs.length *2);
//		for(byte b : xs) {
//			sb.append(b).append(" ");
//		}
//		sb.deleteCharAt(sb.length() -1);
//		return sb.toString();
//	}

	private static Scan mkScan(String user) {
		byte[] startRow = new byte[Md5Utils.MD5_LENGTH + longLength];
		byte[] stopRow  = new byte[Md5Utils.MD5_LENGTH + longLength];
		byte[] userHash = Md5Utils.md5sum(user);
		System.arraycopy(userHash, 0, startRow, 0, userHash.length);
		System.arraycopy(userHash, 0, stopRow, 0, userHash.length);
		stopRow[userHash.length-1]++;

//		System.out.println("Scan starting at: '" + to_str(startRow) + "'");
//		System.out.println("Scan stopping at: '" + to_str(stopRow) + "'");

		Scan s = new Scan(startRow, stopRow);
		s.addColumn(TWITS_FAM, USER_COL);
		s.addColumn(TWITS_FAM, TWIT_COL);
		return s;
	}

	public void postTwit(String user, DateTime dt, String text) throws IOException {

		HTableInterface twits = pool.getTable(TABLE_NAME);

		Put p = mkPut(new Twit(user, dt, text));
		twits.put(p);

		pool.putTable(twits);
	}

	public HBaseIA.TwitBase.model.Twit getTwit(String user, DateTime dt) throws IOException {

		HTableInterface twits = pool.getTable(TABLE_NAME);

		Get g = mkGet(user, dt);
		Result result = twits.get(g);
		if (result.isEmpty())
			return null;

		Twit t = new Twit(result);
		pool.putTable(twits);
		return t;
	}

	public List<HBaseIA.TwitBase.model.Twit> list(String user) throws IOException {

		HTableInterface twits = pool.getTable(TABLE_NAME);

		ResultScanner results = twits.getScanner(mkScan(user));
		List<HBaseIA.TwitBase.model.Twit> ret = new ArrayList<HBaseIA.TwitBase.model.Twit>();
		for(Result r : results) {
			ret.add(new Twit(r));
		}

		pool.putTable(twits);
		return ret;
	}

	private static class Twit extends HBaseIA.TwitBase.model.Twit {

		public Twit(Result r) {
			this(
					r.getColumnLatest(TWITS_FAM, USER_COL).getValue(),
					Arrays.copyOfRange(r.getRow(), Md5Utils.MD5_LENGTH+1, longLength),
					r.getColumnLatest(TWITS_FAM, TWIT_COL).getValue());
		}

		public Twit(byte[] user, byte[] dt, byte[] text) {
			this(
					Bytes.toString(user),
					new DateTime(-1 * Bytes.toLong(dt)),
					Bytes.toString(text));
		}

		public Twit(String user, DateTime dt, String text) {
			this.user = user;
			this.dt = dt;
			this.text = text;
		}
	}
}
