package HBaseIA.TwitBase.coprocessors;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface RelationCountProtocol extends CoprocessorProtocol {
  public long followedByCount(String userId) throws IOException;
}
