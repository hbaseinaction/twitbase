package HBaseIA.TwitBase.coprocessors;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import HBaseIA.TwitBase.hbase.RelationsDAO;

public class FollowsObserver extends BaseRegionObserver {

  private static final Logger log = Logger.getLogger(FollowsObserver.class);
  HTablePool pool = null;

  @Override
  public void postPut(
      final ObserverContext<RegionCoprocessorEnvironment> e,
      final Put put,
      final WALEdit edit,
      final boolean writeToWAL)
    throws IOException {

    log.info("postPut()");
    if (!put.getFamilyMap().containsKey(RelationsDAO.FOLLOWS_FAM))
    	return;

    String from = Bytes.toString(put.get(
      RelationsDAO.FOLLOWS_FAM,
      RelationsDAO.REL_FROM).get(0).getValue());
    String to = Bytes.toString(put.get(
      RelationsDAO.FOLLOWS_FAM,
      RelationsDAO.REL_TO).get(0).getValue());

    log.info(String.format("intercepted new relation: %s -> %s", from, to));
    if (pool == null) {
    	pool = new HTablePool(e.getEnvironment().getConfiguration(), 10);
    }
    RelationsDAO relations = new RelationsDAO(pool);
    relations.addFollowed(to, from);
  }
}
