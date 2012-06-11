package HBaseIA.TwitBase.coprocessors;

import static HBaseIA.TwitBase.hbase.RelationsDAO.FOLLOWS_TABLE_NAME;
import static HBaseIA.TwitBase.hbase.RelationsDAO.FROM;
import static HBaseIA.TwitBase.hbase.RelationsDAO.RELATION_FAM;
import static HBaseIA.TwitBase.hbase.RelationsDAO.TO;

import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import HBaseIA.TwitBase.hbase.RelationsDAO;

public class FollowsObserver extends BaseRegionObserver {

  private HTablePool pool = null;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    pool = new HTablePool(env.getConfiguration(), Integer.MAX_VALUE);
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    pool.close();
  }

  @Override
  public void postPut(
      final ObserverContext<RegionCoprocessorEnvironment> e,
      final Put put,
      final WALEdit edit,
      final boolean writeToWAL)
    throws IOException {

    byte[] table
      = e.getEnvironment().getRegion().getRegionInfo().getTableName();
    if (!Bytes.equals(table, FOLLOWS_TABLE_NAME))
      return;

    KeyValue kv = put.get(RELATION_FAM, FROM).get(0);
    String from = Bytes.toString(kv.getValue());
    kv = put.get(RELATION_FAM, TO).get(0);
    String to = Bytes.toString(kv.getValue());

    RelationsDAO relations = new RelationsDAO(pool);
    relations.addFollowedBy(to, from);
  }
}
