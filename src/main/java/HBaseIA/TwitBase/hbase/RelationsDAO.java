package HBaseIA.TwitBase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import utils.Md5Utils;
import HBaseIA.TwitBase.coprocessors.RelationCountProtocol;

public class RelationsDAO {

  // md5(id_from)md5(id_to) -> 'f':id_to=name_to
  // md5(id_from)md5(id_to) -> 'f':'to'=id_to, 'f':'from'=id_from

  public static final byte[] FOLLOWS_TABLE_NAME = Bytes.toBytes("follows");
  public static final byte[] FOLLOWED_TABLE_NAME = Bytes.toBytes("followedBy");
  public static final byte[] RELATION_FAM = Bytes.toBytes("f");
  public static final byte[] FROM = Bytes.toBytes("from");
  public static final byte[] TO = Bytes.toBytes("to");

  private static final int KEY_WIDTH = 2 * Md5Utils.MD5_LENGTH;

  private HTablePool pool;

  public RelationsDAO(HTablePool pool) {
    this.pool = pool;
  }

  public static byte[] mkRowKey(String a) {
    byte[] ahash = Md5Utils.md5sum(a);
    byte[] rowkey = new byte[KEY_WIDTH];

    Bytes.putBytes(rowkey, 0, ahash, 0, ahash.length);
    return rowkey;
  }

  public static byte[] mkRowKey(String a, String b) {
    byte[] ahash = Md5Utils.md5sum(a);
    byte[] bhash = Md5Utils.md5sum(b);
    byte[] rowkey = new byte[KEY_WIDTH];

    int offset = 0;
    offset = Bytes.putBytes(rowkey, offset, ahash, 0, ahash.length);
    Bytes.putBytes(rowkey, offset, bhash, 0, bhash.length);
    return rowkey;
  }

  public static byte[][] splitRowkey(byte[] rowkey) {
    byte[][] result = new byte[2][];

    result[0] = Arrays.copyOfRange(rowkey, 0, Md5Utils.MD5_LENGTH);
    result[1] = Arrays.copyOfRange(rowkey, Md5Utils.MD5_LENGTH, KEY_WIDTH);
    return result;
  }

  public void addFollows(String fromId, String toId) throws IOException {
    addRelation(FOLLOWS_TABLE_NAME, fromId, toId);
  }

  public void addFollowedBy(String fromId, String toId) throws IOException {
    addRelation(FOLLOWED_TABLE_NAME, fromId, toId);
  }

  public void addRelation(byte[] table, String fromId, String toId) throws IOException {

    HTableInterface t = pool.getTable(table);

    Put p = new Put(mkRowKey(fromId, toId));
    p.add(RELATION_FAM, FROM, Bytes.toBytes(fromId));
    p.add(RELATION_FAM, TO, Bytes.toBytes(toId));
    t.put(p);

    t.close();
  }

  public List<HBaseIA.TwitBase.model.Relation> listFollows(String fromId) throws IOException {
    return listRelations(FOLLOWS_TABLE_NAME, fromId);
  }

  public List<HBaseIA.TwitBase.model.Relation> listFollowedBy(String fromId) throws IOException {
    return listRelations(FOLLOWED_TABLE_NAME, fromId);
  }

  public List<HBaseIA.TwitBase.model.Relation> listRelations(byte[] table, String fromId) throws IOException {

    HTableInterface t = pool.getTable(table);
    String rel = (Bytes.equals(table, FOLLOWS_TABLE_NAME)) ? "->" : "<-";

    byte[] startKey = mkRowKey(fromId);
    byte[] endKey = Arrays.copyOf(startKey, startKey.length);
    endKey[Md5Utils.MD5_LENGTH-1]++;
    Scan scan = new Scan(startKey, endKey);
    scan.addColumn(RELATION_FAM, TO);
    scan.setMaxVersions(1);

    ResultScanner results = t.getScanner(scan);
    List<HBaseIA.TwitBase.model.Relation> ret
      = new ArrayList<HBaseIA.TwitBase.model.Relation>();
    for (Result r : results) {
      KeyValue kv = r.getColumnLatest(RELATION_FAM, TO);
      String toId = Bytes.toString(kv.getValue());
      ret.add(new Relation(rel, fromId, toId));
    }

    t.close();
    return ret;
  }

  @SuppressWarnings("unused")
  public long followedByCountScan (String user) throws IOException {
    HTableInterface followed = pool.getTable(FOLLOWED_TABLE_NAME);

    final byte[] startKey = Md5Utils.md5sum(user);
    final byte[] endKey = Arrays.copyOf(startKey, startKey.length);
    endKey[endKey.length-1]++;
    Scan scan = new Scan(startKey, endKey);
    scan.setMaxVersions(1);

    long sum = 0;
    ResultScanner rs = followed.getScanner(scan);
    for(Result r : rs) {
      sum++;
    }
    return sum;
  }

  public long followedByCount (final String userId) throws Throwable {
    HTableInterface followed = pool.getTable(FOLLOWED_TABLE_NAME);

    final byte[] startKey = Md5Utils.md5sum(userId);
    final byte[] endKey = Arrays.copyOf(startKey, startKey.length);
    endKey[endKey.length-1]++;

    Batch.Call<RelationCountProtocol, Long> callable =
      new Batch.Call<RelationCountProtocol, Long>() {
        @Override
        public Long call(RelationCountProtocol instance)
            throws IOException {
          return instance.followedByCount(userId);
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
