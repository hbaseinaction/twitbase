package HBaseIA.TwitBase.coprocessors;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import HBaseIA.TwitBase.hbase.FollowersDAO;

public class FollowersObserver extends BaseRegionObserver {

  HTablePool pool = null;

  @Override
  public void postPut(
      final ObserverContext<RegionCoprocessorEnvironment> e,
      final Put put,
      final WALEdit edit,
      final boolean writeToWAL)
    throws IOException {

    if (!put.getFamilyMap().containsKey(FollowersDAO.FOLLOWERS_FAM))
    	return;

    byte[] rowkey = put.getRow();
    byte[][] splits = FollowersDAO.splitRowkey(rowkey); // [->, user1, user2]
    if (!Arrays.equals(splits[0], FollowersDAO.FOLLOWS_RELATION))
      return;

    String from = Bytes.toString(put.get(
      FollowersDAO.FOLLOWERS_FAM,
      FollowersDAO.REL_FROM).get(0).getValue());
    String to = Bytes.toString(put.get(
      FollowersDAO.FOLLOWERS_FAM,
      FollowersDAO.REL_TO).get(0).getValue());

    if (pool == null) {
    	pool = new HTablePool(e.getEnvironment().getConfiguration(), 10);
    }
    FollowersDAO followers = new FollowersDAO(pool);
    followers.addFollowing(to, from);
  }
}
