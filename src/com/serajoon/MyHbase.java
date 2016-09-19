package com.serajoon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class MyHbase {
	private static Configuration conf;
	private static Connection conn;
	private static Admin admin;

	// private static String QUOREM =
	// "192.168.75.128,192.168.75.129,192.168.75.130";// zookeeper
	static {
		try {
			conf = HBaseConfiguration.create();
			// conf.set("hbase.zookeeper.quorum", QUOREM);// ����Zookeeper
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * �ر����� �ͷ���Դ
	 */
	public void close() {
		try {
			if (admin != null) {
				admin.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * ������
	 * 
	 * @param tableName
	 *            ����
	 * @param columnFamily
	 *            ����
	 */
	public void createTable(String tableName, String[] columnFamily) {
		try {
			TableName tn = TableName.valueOf(tableName);
			if (admin.tableExists(tn)) {
				System.out.println("���Ѿ�����");
			} else {
				HTableDescriptor hTableDescriptor = new HTableDescriptor(tn);
				for (String cf : columnFamily) {
					hTableDescriptor.addFamily(new HColumnDescriptor(cf));
				}
				admin.createTable(hTableDescriptor);
				System.out.println("������ɹ�");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * ɾ����
	 * 
	 * @param tableName
	 *            ����
	 */
	public void deleteTable(String tableName) {
		try {
			TableName tn = TableName.valueOf(tableName);
			if (admin.tableExists(tn)) {
				admin.disableTable(tn);
				admin.deleteTable(tn);
				System.out.println("ɾ����ɹ�");
			} else {
				System.out.println("�����ڣ�ɾ��ʧ��");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * �鿴���б�
	 */
	public void listTables() {
		try {
			TableName[] listTableNames = admin.listTableNames();
			for (TableName t : listTableNames) {
				System.out.println(t.getNameAsString());
			}
			System.out.println("���");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * �鿴ָ����������Ƿ����
	 * 
	 * @param tableName
	 * @param colFamliyName
	 */
	public boolean hasColumnFamily(String tableName, String colFamliyName) {
		TableName tn = TableName.valueOf(tableName);
		boolean hasFamily = false;
		try {
			HTableDescriptor tableDescriptor = admin.getTableDescriptor(tn);
			hasFamily = tableDescriptor.hasFamily(Bytes.toBytes(colFamliyName));
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hasFamily;
	}

	/**
	 * ������� Ĭ�ϰ汾��Ϊ1
	 * 
	 * @param tableName
	 *            ����
	 * @param colFamliyName
	 *            ������
	 */
	public void addColumnFamily(String tableName, String colFamliyName) {
		try {
			TableName tn = TableName.valueOf(tableName);
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(colFamliyName);
			if (hasColumnFamily(tableName, colFamliyName)) {
				System.out.println("��" + tableName + "�Ѿ���������" + colFamliyName);
			} else {
				admin.addColumn(tn, columnDescriptor);
				System.out.println("��" + tableName + "�������" + colFamliyName + "�ɹ�");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void addColumnFamily(String tableName, String colFamliyName, int version) {
		try {
			TableName tn = TableName.valueOf(tableName);
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(colFamliyName);
			if (version > 0) {
				columnDescriptor.setMaxVersions(version);
			}
			if (hasColumnFamily(tableName, colFamliyName)) {
				System.out.println("��" + tableName + "�Ѿ���������" + colFamliyName);
			} else {
				admin.addColumn(tn, columnDescriptor);
				System.out.println("��" + tableName + "�������" + colFamliyName + "�ɹ�");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * ɾ������
	 * 
	 * @param tableName
	 *            ����
	 * @param colFamliyName
	 *            ������
	 */
	public void deleteColumnFamily(String tableName, String colFamliyName) {
		try {
			TableName tn = TableName.valueOf(tableName);
			if (hasColumnFamily(tableName, colFamliyName)) {
				admin.deleteColumn(tn, Bytes.toBytes(colFamliyName));
				System.out.println("��" + tableName + "ɾ������" + colFamliyName + "�ɹ�");
			} else {
				System.out.println("��" + tableName + "����������" + colFamliyName);
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated cat ch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * ����һ������
	 * 
	 * @param tableName
	 *            ����
	 * @param colFamily
	 *            ������
	 * @param col
	 *            ����
	 * @param val
	 *            ֵ
	 */
	public void insertData(String tableName, String rowkey, String colFamily, String col, String val) {
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Put p = null;
			if (rowkey == null) {// ����
				p = new Put(Bytes.toBytes(UUIDUtil.getUUID()));// row key
			} else {// �޸�
				p = new Put(Bytes.toBytes(rowkey));// row key
			}
			p.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
			table.put(p);
			System.out.println("���ݲ���ɹ�");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * ��ȡָ���е�����
	 * 
	 * @param rowkey
	 * @param tableName
	 * @param colFamily
	 * @param col
	 */
	public void getColumnValue(String rowkey, String tableName, String colFamily, String col, int version) {
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowkey));
			get.setMaxVersions(version);
			get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
			Result result = table.get(get);
			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) {
				String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
				String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
						"UTF-8");// ����
				String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
						cell.getQualifierLength(), "UTF-8");// ����
				String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), "UTF-8");// ����
				System.out.println(rowKey + " " + family + " " + column + " " + value);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void getColumnValue(String rowkey, String tableName, String colFamily, String col) {
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowkey));
			get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
			Result result = table.get(get);
			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) {
				String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
				String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
						"UTF-8");// ����
				String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
						cell.getQualifierLength(), "UTF-8");// ����
				String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), "UTF-8");// ����
				System.out.println(rowKey + " " + family + " " + column + " " + value);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * ɾ��ָ����Ԫ�������
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param familyName
	 * @param col
	 */
	public void deleteCell(String tableName, String rowkey, String familyName, String col) {
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			delete.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(col));
			table.delete(delete);
			table.close();
			System.out.println("ɾ���ɹ�");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteRow(String tableName, String rowkey) {
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			table.delete(delete);
			table.close();
			System.out.println("ɾ���ɹ�");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteAll(String tableName) {
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			List<Delete> deleteList = new ArrayList<Delete>();
			Delete delete = null;
			while (it.hasNext()) {
				for (Cell cell : it.next().rawCells()) {
					String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
					delete = new Delete(Bytes.toBytes(rowKey));
					deleteList.add(delete);
				}
			}
			table.delete(deleteList);
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// ��ѯ��������
	public void getAllRows(String tableName, String colFamily, String col) {
		try {
			Formatter f = new Formatter(System.out);
			f.format("%-15s %10s %10s %10s %10s", "rowkey", "family", "column", "value", "timestamp");
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			System.out.println();
			while (it.hasNext()) {
				for (Cell cell : it.next().rawCells()) {
					String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
					String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
							"UTF-8");// ����
					String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength(), "UTF-8");// ����
					String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
							"UTF-8");// ����

					f.format("%-15s %10s %10s %10s %10s", rowKey, family, column, value, cell.getTimestamp());
					System.out.println();
				}
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void getRowsFromTo(String tableName, String startRow, String endRow) {
		try {
			Formatter f = new Formatter(System.out);
			f.format("%-15s %10s %10s %10s %10s", "rowkey", "family", "column", "value", "timestamp");
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			System.out.println();
			while (it.hasNext()) {
				for (Cell cell : it.next().rawCells()) {
					String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
					String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
							"UTF-8");// ����
					String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength(), "UTF-8");// ����
					String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
							"UTF-8");// ����

					f.format("%-15s %10s %10s %10s %10s", rowKey, family, column, value, cell.getTimestamp());
					System.out.println();
				}
			}
			scanner.close();
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// ����������ѯ����ѯ������¼
	public void getManyRows(String tableName) {
		Formatter f = new Formatter(System.out);
		f.format("%-15s %10s %10s %10s %10s", "rowkey", "family", "column", "value", "timestamp");
		System.out.println();
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes("1"), null, CompareOp.EQUAL,
					Bytes.toBytes("hanmeng")); // ����column1��ֵΪaaaʱ���в�ѯ
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				List<Cell> listCells = r.listCells();
				for (Cell cell : listCells) {
					String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
					String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
							"UTF-8");// ����
					String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength(), "UTF-8");// ����
					String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
							"UTF-8");// ����
					System.out.println(rowKey + " " + family + " " + column + " " + value);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void singleColumnValueFilter(String tableName, String colFamily, String col, String val) {
		try {
			Formatter f = null;
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			SingleColumnValueFilter f1 = new SingleColumnValueFilter(Bytes.toBytes(colFamily), Bytes.toBytes(col),CompareOp.EQUAL, Bytes.toBytes(val));
			f1.setFilterIfMissing(true);
			filterList.addFilter(f1);
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setFilter(filterList);
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			if (it.hasNext()) {
				f = new Formatter(System.out);
				f.format("%-32s %-32s %-15s %-15s %-15s", "rowkey", "family", "column", "value", "timestamp");
				System.out.println();
			}
			while (it.hasNext()) {
				for (Cell cell : it.next().rawCells()) {
					String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
					String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
							"UTF-8");// ����
					String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength(), "UTF-8");// ����
					String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
							"UTF-8");// ����

					f.format("%-32s %-32s %-15s %-15s %-15s", rowKey, family, column, value, cell.getTimestamp());
					System.out.println();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void singleColumnValueFilterByteArrayComparable(String tableName, String colFamily, String col, String val) {
		try {
			Formatter f = null;
			RegexStringComparator comp = new RegexStringComparator(val + ".");
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			SingleColumnValueFilter f1 = new SingleColumnValueFilter(Bytes.toBytes(colFamily), Bytes.toBytes(col),
					CompareOp.EQUAL, comp);
			f1.setFilterIfMissing(true);
			filterList.addFilter(f1);
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setFilter(filterList);
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			if (it.hasNext()) {
				f = new Formatter(System.out);
				f.format("%-32s %-32s %-15s %-15s %-15s", "rowkey", "family", "column", "value", "timestamp");
				System.out.println();
			}
			while (it.hasNext()) {
				for (Cell cell : it.next().rawCells()) {
					String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
					String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
							"UTF-8");// ����
					String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength(), "UTF-8");// ����
					String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
							"UTF-8");// ����

					f.format("%-32s %-32s %-15s %-15s %-15s", rowKey, family, column, value, cell.getTimestamp());
					System.out.println();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * ��ֵ������
	 * @param tableName ����
	 * @param colFamily ����
	 * @param col		��
	 * @param val		ֵ
	 */
	public void singleColumnValueFilterSubStringComparable(String tableName, String colFamily, String col, String val) {
		try {
			Formatter f = null;
			SubstringComparator comp = new SubstringComparator(val);
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			SingleColumnValueFilter f1 = new SingleColumnValueFilter(Bytes.toBytes(colFamily), Bytes.toBytes(col),CompareOp.EQUAL, comp);
			f1.setFilterIfMissing(true);
			filterList.addFilter(f1);
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setFilter(filterList);
			//scan.addFamily(Bytes.toBytes(colFamily));
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			if (it.hasNext()) {
				f = new Formatter(System.out);
				f.format("%-32s %-32s %-15s %-15s %-15s", "rowkey", "family", "column", "value", "timestamp");
				System.out.println();
			}
			while (it.hasNext()) {
				for (Cell cell : it.next().rawCells()) {
					String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
					String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
							"UTF-8");// ����
					String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength(), "UTF-8");// ����
					String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
							"UTF-8");// ����

					f.format("%-32s %-32s %-15s %-15s %-15s", rowKey, family, column, value, cell.getTimestamp());
					System.out.println();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * ���������
	 * @param tableName ����
	 * @param colFamily ������
	 */
	public void familyFilter(String tableName, String colFamily) {
		try {
			Formatter f = null;
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			FamilyFilter ff = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(colFamily)));
			filterList.addFilter(ff);
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setFilter(filterList);
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			if (it.hasNext()) {
				f = new Formatter(System.out);
				f.format("%-32s %-32s %-15s %-15s %-15s", "rowkey", "family", "column", "value", "timestamp");
				System.out.println();
			}
			while (it.hasNext()) {
				for (Cell cell : it.next().rawCells()) {
					String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
					String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
							"UTF-8");// ����
					String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength(), "UTF-8");// ����
					String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
							"UTF-8");// ����

					f.format("%-32s %-32s %-15s %-15s %-15s", rowKey, family, column, value, cell.getTimestamp());
					System.out.println();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * ����ǰ׺������
	 * @param tableName ����
	 * @param col		����
	 */
	public void columnPrefixFilter(String tableName, String col) {
		try {
			Formatter f = null;
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter ff =new ColumnPrefixFilter(Bytes.toBytes(col));
			filterList.addFilter(ff);
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setFilter(filterList);
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			if (it.hasNext()) {
				f = new Formatter(System.out);
				f.format("%-32s %-32s %-15s %-15s %-15s", "rowkey", "family", "column", "value", "timestamp");
				System.out.println();
			}
			while (it.hasNext()) {
				for (Cell cell : it.next().rawCells()) {
					String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
					String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
							"UTF-8");// ����
					String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength(), "UTF-8");// ����
					String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
							"UTF-8");// ����

					f.format("%-32s %-32s %-15s %-15s %-15s", rowKey, family, column, value, cell.getTimestamp());
					System.out.println();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * ��ֵ������
	 * @param tableName ����
	 * @param col		����
	 */
	public void rowFilter(String tableName, String rowkey) {
		try {
			Formatter f = null;
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			Filter ff =new RowFilter(CompareOp.EQUAL,new SubstringComparator(rowkey));
			filterList.addFilter(ff);
			Filter ff1=new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("basicinfo")));
			filterList.addFilter(ff1);
			Filter ff2=new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("age")));
			filterList.addFilter(ff2);
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setFilter(filterList);
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> it = scanner.iterator();
			if (it.hasNext()) {
				f = new Formatter(System.out);
				f.format("%-32s %-32s %-15s %-15s %-15s", "rowkey", "family", "column", "value", "timestamp");
				System.out.println();
			}
			while (it.hasNext()) {
				for (Cell cell : it.next().rawCells()) {
					String rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8");// �н�
					String family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
							"UTF-8");// ����
					String column = new String(cell.getQualifierArray(), cell.getQualifierOffset(),
							cell.getQualifierLength(), "UTF-8");// ����
					String value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
							"UTF-8");// ����

					f.format("%-32s %-32s %-15s %-15s %-15s", rowKey, family, column, value, cell.getTimestamp());
					System.out.println();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// ValueFilter
	public void valueFilter(String tableName, String value) {
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			scan.setFilter(new ValueFilter(CompareOp.EQUAL, new SubstringComparator(value)));
			ResultScanner scanner = table.getScanner(scan);
			for (Result r : scanner) {
				System.out.println(Bytes.toString(r.getRow()));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
