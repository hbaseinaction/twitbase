package HBaseIA.TwitBase.coprocessors;

import static HBaseIA.TwitBase.hbase.RelationsDAO.FROM;
import static HBaseIA.TwitBase.hbase.RelationsDAO.RELATION_FAM;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import utils.Md5Utils;

public class RelationCountImpl
  extends BaseEndpointCoprocessor implements RelationCountProtocol {

  @Override
  public long followedByCount(String userId) throws IOException {
    byte[] startkey = Md5Utils.md5sum(userId);
    Scan scan = new Scan(startkey);
    scan.setFilter(new PrefixFilter(startkey));
    scan.addColumn(RELATION_FAM, FROM);
    scan.setMaxVersions(1);

    RegionCoprocessorEnvironment env
      = (RegionCoprocessorEnvironment)getEnvironment();
    InternalScanner scanner = env.getRegion().getScanner(scan);

    long sum = 0;
    List<KeyValue> results = new ArrayList<KeyValue>();
    boolean hasMore = false;
    do {
      hasMore = scanner.next(results);
      sum += results.size();
      results.clear();
    } while (hasMore);
    scanner.close();
    return sum;
  }
}
