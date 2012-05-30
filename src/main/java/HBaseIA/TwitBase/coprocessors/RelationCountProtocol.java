package HBaseIA.TwitBase.coprocessors;

import java.io.IOException;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface RelationCountProtocol extends CoprocessorProtocol {
  public long count(byte[] startKey, byte[] endKey) throws IOException;
}
