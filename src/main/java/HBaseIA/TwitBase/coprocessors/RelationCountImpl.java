package HBaseIA.TwitBase.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import HBaseIA.TwitBase.hbase.FollowersDAO;

public class RelationCountImpl
  extends BaseEndpointCoprocessor implements RelationCountProtocol {

  @Override
  public long count(byte[] startKey, byte[] endKey) throws IOException {
    Scan scan = new Scan(startKey, endKey);
    InternalScanner scanner =
      ((RegionCoprocessorEnvironment) getEnvironment())
      .getRegion().getScanner(scan);
    long sum = 0;
    List<KeyValue> results = new ArrayList<KeyValue>();
    boolean hasMore = false;
    do {
      hasMore = scanner.next(results);
      for (KeyValue kv : results) {
        // a kv is returned for each cell. only count each row once.
        // TODO: use kv.getBuffer() instead
        if (Bytes.equals(FollowersDAO.FOLLOWERS_FAM, kv.getFamily()) &&
            Bytes.equals(FollowersDAO.REL_FROM, kv.getQualifier())) {
          sum++;
        }
      }
      results.clear();
    } while (hasMore);
    return sum;
  }
}
