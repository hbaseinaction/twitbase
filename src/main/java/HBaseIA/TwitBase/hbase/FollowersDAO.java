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

/**
 * TODO: complete me with Ch05. Don't forget to update
 * FollowersObserver and Ch06 as appropriate.
 */
public class FollowersDAO {

  public static final byte[] TABLE_NAME = Bytes.toBytes("followers");
  public static final byte[] FOLLOWERS_FAM = Bytes.toBytes("f");
  // TODO: these names are terrible. please advise.
  public static final byte[] FOLLOWS_RELATION   = Bytes.toBytes("->");
  public static final byte[] FOLLOWING_RELATION = Bytes.toBytes("<-");
  public static final byte[] REL_FROM = Bytes.toBytes("from");
  public static final byte[] REL_TO   = Bytes.toBytes("to");

  private static final int FOLLOWS_RK_LEN = (2 * Md5Utils.MD5_LENGTH) + FOLLOWS_RELATION.length;
  private static final int FOLLOWING_RK_LEN = (2 * Md5Utils.MD5_LENGTH) + FOLLOWING_RELATION.length;

  private HTablePool pool;

  public FollowersDAO(HTablePool pool) {
    this.pool = pool;
  }

  public static byte[] mkFollowsRowKey(String a, String b) {
    byte[] ahash = Md5Utils.md5sum(a);
    byte[] bhash = Md5Utils.md5sum(b);
    byte[] rowkey = new byte[FOLLOWS_RK_LEN];

    int offset = 0;
    offset = Bytes.putBytes(rowkey, 0, FOLLOWS_RELATION, 0, FOLLOWS_RELATION.length);
    offset = Bytes.putBytes(rowkey, offset, ahash, 0, ahash.length);
    Bytes.putBytes(rowkey, offset, bhash, 0, bhash.length);
    return rowkey;
  }

  public static byte[] mkFollowingRowKey(String b, String a) {
    byte[] bhash = Md5Utils.md5sum(b);
    byte[] ahash = Md5Utils.md5sum(a);
    byte[] rowkey = new byte[FOLLOWING_RK_LEN];

    int offset = 0;
    offset = Bytes.putBytes(rowkey, 0, FOLLOWING_RELATION, 0, FOLLOWING_RELATION.length);
    offset = Bytes.putBytes(rowkey, offset, bhash, 0, bhash.length);
    Bytes.putBytes(rowkey, offset, ahash, 0, ahash.length);
    return rowkey;
  }

  public static byte[][] splitRowkey(byte[] rowkey) {
    byte[][] result = new byte[3][];
    if (Bytes.startsWith(rowkey, FOLLOWS_RELATION)) {
      result[0] = Arrays.copyOf(rowkey, FOLLOWS_RELATION.length);
      result[1] = Arrays.copyOfRange(rowkey, FOLLOWS_RELATION.length, FOLLOWS_RELATION.length + Md5Utils.MD5_LENGTH);
      result[2] = Arrays.copyOfRange(rowkey, FOLLOWS_RELATION.length + Md5Utils.MD5_LENGTH, FOLLOWS_RK_LEN);
    } else if (Bytes.startsWith(rowkey, FOLLOWING_RELATION)) {
      result[0] = Arrays.copyOf(rowkey, FOLLOWING_RELATION.length);
      result[1] = Arrays.copyOfRange(rowkey, FOLLOWING_RELATION.length, FOLLOWING_RELATION.length + Md5Utils.MD5_LENGTH);
      result[2] = Arrays.copyOfRange(rowkey, FOLLOWING_RELATION.length + Md5Utils.MD5_LENGTH, FOLLOWING_RK_LEN);
    } else {
      throw new IllegalArgumentException();
    }
    return result;
  }

  public void addFollows(String from, String to) throws IOException {

    HTableInterface followers = pool.getTable(TABLE_NAME);

    Put p = new Put(mkFollowsRowKey(from, to));
    p.add(FOLLOWERS_FAM, REL_FROM, Bytes.toBytes(from));
    p.add(FOLLOWERS_FAM, REL_TO, Bytes.toBytes(to));
    followers.put(p);

    followers.close();
  }

  public void addFollowing(String from, String to) throws IOException {

    HTableInterface followers = pool.getTable(TABLE_NAME);

    Put p = new Put(mkFollowingRowKey(from, to));
    p.add(FOLLOWERS_FAM, REL_FROM, Bytes.toBytes(from));
    p.add(FOLLOWERS_FAM, REL_TO, Bytes.toBytes(to));
    followers.put(p);

    followers.close();
  }

  public List<HBaseIA.TwitBase.model.Relation> listRelations() throws IOException {

    HTableInterface followers = pool.getTable(TABLE_NAME);

    ResultScanner results = followers.getScanner(FOLLOWERS_FAM);
    List<HBaseIA.TwitBase.model.Relation> ret = new ArrayList<HBaseIA.TwitBase.model.Relation>();
    for (Result r : results) {
      ret.add(new Relation(r));
    }

    followers.close();
    return ret;
  }

  public long followersCountScan (String user) throws IOException {
    HTableInterface followers = pool.getTable(TABLE_NAME);

    final byte[] startKey = new byte[FOLLOWING_RK_LEN];
    int offset = 0;
    offset = Bytes.putBytes(
      startKey,
      offset,
      FOLLOWING_RELATION,
      0,
      FOLLOWING_RELATION.length);
    offset = Bytes.putBytes(
      startKey,
      offset,
      Md5Utils.md5sum(user),
      0,
      Md5Utils.MD5_LENGTH);

    final byte[] endKey = new byte[FOLLOWING_RK_LEN];
    Bytes.putBytes(endKey, 0, startKey, 0, offset);
    endKey[offset -1]++;

    long sum = 0;
    ResultScanner rs = followers.getScanner(new Scan(startKey, endKey));
    for(Result r : rs) {
      sum++;
    }
    return sum;
  }

  public long followersCount (String user) throws Throwable {
    HTableInterface followers = pool.getTable(TABLE_NAME);

    final byte[] startKey = new byte[FOLLOWING_RK_LEN];
    int offset = 0;
    offset = Bytes.putBytes(
      startKey,
      offset,
      FOLLOWING_RELATION,
      0,
      FOLLOWING_RELATION.length);
    offset = Bytes.putBytes(
      startKey,
      offset,
      Md5Utils.md5sum(user),
      0,
      Md5Utils.MD5_LENGTH);

    final byte[] endKey = new byte[FOLLOWING_RK_LEN];
    Bytes.putBytes(endKey, 0, startKey, 0, offset);
    endKey[offset -1]++;

    Batch.Call<RelationCountProtocol, Long> callable =
      new Batch.Call<RelationCountProtocol, Long>() {
      @Override
      public Long call(RelationCountProtocol instance)
      throws IOException {
        return instance.count(startKey, endKey);
      }
    };

    Map<byte[], Long> results =
      followers.coprocessorExec(
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

    private Relation(Result r) {
      this(
           splitRowkey(r.getRow())[0],
           r.getColumnLatest(FOLLOWERS_FAM, REL_FROM).getValue(),
           r.getColumnLatest(FOLLOWERS_FAM, REL_TO).getValue());
    }

    private Relation(byte[] relation, byte[] from, byte[] to) {
      this(
           Bytes.toString(relation),
           Bytes.toString(from),
           Bytes.toString(to));
    }

    private Relation(String relation, String from, String to) {
      this.relation = relation;
      this.from = from;
      this.to = to;
    }
  }
}
