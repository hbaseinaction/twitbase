package HBaseIA.TwitBase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import HBaseIA.TwitBase.Md5Utils;
import HBaseIA.TwitBase.coprocessors.RelationCountProtocol;

public class RelationsDAO {

  public static final byte[] FOLLOWS_TABLE_NAME = Bytes.toBytes("follows");
  public static final byte[] FOLLOWED_TABLE_NAME = Bytes.toBytes("followed");
  public static final byte[] FOLLOWS_FAM = Bytes.toBytes("follows");
  public static final byte[] FOLLOWED_FAM = Bytes.toBytes("followed");

  public static final byte[] REL_FROM = Bytes.toBytes("from");
  public static final byte[] REL_TO   = Bytes.toBytes("to");

  private static final int KEY_WIDTH = 2 * Md5Utils.MD5_LENGTH;

  private HTablePool pool;

  public RelationsDAO(HTablePool pool) {
    this.pool = pool;
  }

  public static byte[] mkFollowsRowKey(String a, String b) {
    byte[] ahash = Md5Utils.md5sum(a);
    byte[] bhash = Md5Utils.md5sum(b);
    byte[] rowkey = new byte[KEY_WIDTH];

    int offset = 0;
    offset = Bytes.putBytes(rowkey, offset, ahash, 0, ahash.length);
    Bytes.putBytes(rowkey, offset, bhash, 0, bhash.length);
    return rowkey;
  }

  public static byte[] mkFollowedRowKey(String b, String a) {
    byte[] bhash = Md5Utils.md5sum(b);
    byte[] ahash = Md5Utils.md5sum(a);
    byte[] rowkey = new byte[KEY_WIDTH];

    int offset = 0;
    offset = Bytes.putBytes(rowkey, offset, bhash, 0, bhash.length);
    Bytes.putBytes(rowkey, offset, ahash, 0, ahash.length);
    return rowkey;
  }

  public static byte[][] splitRowkey(byte[] rowkey) {
    byte[][] result = new byte[2][];

    result[0] = Arrays.copyOfRange(rowkey, 0, Md5Utils.MD5_LENGTH);
    result[1] = Arrays.copyOfRange(rowkey, Md5Utils.MD5_LENGTH, KEY_WIDTH);
    return result;
  }

  public void addFollows(String from, String to) throws IOException {

    HTableInterface followers = pool.getTable(FOLLOWS_TABLE_NAME);

    Put p = new Put(mkFollowsRowKey(from, to));
    p.add(FOLLOWS_FAM, REL_FROM, Bytes.toBytes(from));
    p.add(FOLLOWS_FAM, REL_TO, Bytes.toBytes(to));
    followers.put(p);

    followers.close();
  }

  public void addFollowed(String from, String to) throws IOException {

    HTableInterface followed = pool.getTable(FOLLOWED_TABLE_NAME);

    Put p = new Put(mkFollowedRowKey(from, to));
    p.add(FOLLOWED_FAM, REL_FROM, Bytes.toBytes(from));
    p.add(FOLLOWED_FAM, REL_TO, Bytes.toBytes(to));
    followed.put(p);

    followed.close();
  }

  public List<HBaseIA.TwitBase.model.Relation> listFollows() throws IOException {

    HTableInterface follows = pool.getTable(FOLLOWS_TABLE_NAME);

    ResultScanner results = follows.getScanner(FOLLOWS_FAM);
    List<HBaseIA.TwitBase.model.Relation> ret
      = new ArrayList<HBaseIA.TwitBase.model.Relation>();
    for (Result r : results) {
      String from = Bytes.toString(
        r.getColumnLatest(FOLLOWS_FAM, REL_FROM).getValue());
      String to = Bytes.toString(
        r.getColumnLatest(FOLLOWS_FAM, REL_TO).getValue());
      ret.add(new Relation("->", from, to));
    }

    follows.close();
    return ret;
  }

  public List<HBaseIA.TwitBase.model.Relation> listFollowed() throws IOException {

    HTableInterface followed = pool.getTable(FOLLOWED_TABLE_NAME);

    ResultScanner results = followed.getScanner(FOLLOWED_FAM);
    List<HBaseIA.TwitBase.model.Relation> ret
      = new ArrayList<HBaseIA.TwitBase.model.Relation>();
    for (Result r : results) {
      String from = Bytes.toString(
        r.getColumnLatest(FOLLOWED_FAM, REL_FROM).getValue());
      String to = Bytes.toString(
        r.getColumnLatest(FOLLOWED_FAM, REL_TO).getValue());
      ret.add(new Relation("<-", from, to));
    }

    followed.close();
    return ret;
  }

  @SuppressWarnings("unused")
  public long followedCountScan (String user) throws IOException {
    HTableInterface followed = pool.getTable(FOLLOWED_TABLE_NAME);

    final byte[] startKey = Md5Utils.md5sum(user);
    final byte[] endKey = Arrays.copyOf(startKey, startKey.length);
    endKey[endKey.length-1]++;

    long sum = 0;
    ResultScanner rs = followed.getScanner(new Scan(startKey, endKey));
    for(Result r : rs) {
      sum++;
    }
    return sum;
  }

  public long followedCount (String user) throws Throwable {
    HTableInterface followed = pool.getTable(FOLLOWED_TABLE_NAME);

    final byte[] startKey = Md5Utils.md5sum(user);
    final byte[] endKey = Arrays.copyOf(startKey, startKey.length);
    endKey[endKey.length-1]++;

    Batch.Call<RelationCountProtocol, Long> callable =
      new Batch.Call<RelationCountProtocol, Long>() {
      @Override
      public Long call(RelationCountProtocol instance)
      throws IOException {
        return instance.followedCount(startKey, endKey);
      }
    };

    Map<byte[], Long> results =
      followed.coprocessorExec(
        RelationCountProtocol.class,
        startKey,
        endKey,
        callable);

    long sum = 0;
    for(Map.Entry<byte[], Long> e : results.entrySet()) {
      sum += e.getValue().longValue();
    }
    return sum;
  }

  private static class Relation extends HBaseIA.TwitBase.model.Relation {

    private Relation(String relation, String from, String to) {
      this.relation = relation;
      this.from = from;
      this.to = to;
    }
  }
}
