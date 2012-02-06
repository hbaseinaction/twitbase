package HBaseIA.TwitBase.cli;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import HBaseIA.TwitBase.hbase.TwitsDAO;
import HBaseIA.TwitBase.hbase.UsersDAO;

public class InitTables {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		if (admin.tableExists(UsersDAO.TABLE_NAME)) {
			System.out.println("User table already exisis.");
		} else {
			System.out.println("Creating User table...");
			HTableDescriptor desc = new HTableDescriptor(UsersDAO.TABLE_NAME);
			HColumnDescriptor c = new HColumnDescriptor(UsersDAO.INFO_FAM);
			c.setMaxVersions(0);
			desc.addFamily(c);
			admin.createTable(desc);
			System.out.println("User table created.");
		}

		if (admin.tableExists(TwitsDAO.TABLE_NAME)) {
			System.out.println("Twits table already exisis.");
		} else {
			System.out.println("Creating Twits table...");
			HTableDescriptor desc = new HTableDescriptor(TwitsDAO.TABLE_NAME);
			HColumnDescriptor c = new HColumnDescriptor(TwitsDAO.TWITS_FAM);
			c.setMaxVersions(0);
			desc.addFamily(c);
			admin.createTable(desc);
			System.out.println("Twits table created.");
		}
	}
}
