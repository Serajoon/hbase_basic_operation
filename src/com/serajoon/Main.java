package com.serajoon;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Main {
	private MyHbase myhbase;
	@Before
	public void init(){
		myhbase = new MyHbase();
	}
	@After
	public void release(){
		myhbase.close();
		System.out.println("Ö´ÐÐÍê±Ï");
	}
	@Test
	public void createTable(){
		String tableName="info";
		String[] colFamily ={"basicinfo","educationbackground"};
		myhbase.createTable(tableName,colFamily);
	}
	
	@Test
	public void deleteTable(){
		myhbase.deleteTable("TEST");
	}
	@Test
	public void listTables(){
		myhbase.listTables();
	}
	@Test
	public void addColFamily(){
		myhbase.addColumnFamily("info", "hanmeng",-1);
	}
	@Test
	public void deleteColFamily(){
		myhbase.deleteColumnFamily("info", "hanmeng");
	}
	@Test
	public void insertData(){
		/*for(int i=0;i<3;i++){
			myhbase.insertData("info",null,"basicinfo","age",""+i);
		}*/
		myhbase.insertData("info","4","basicinfo","name","row4");
	}
	@Test
	public void getColumn(){
		myhbase.getColumnValue("1","info","basicinfo","name",3);
	}
	@Test
	public void deleteCell(){
		myhbase.deleteCell("info","aaa","basicinfo","age");
	}
	@Test
	public void deleteRow(){
		myhbase.deleteRow("info","aaa");
	}
	
	@Test
	public void deleteAll(){
		myhbase.deleteAll("info");
	}
	
	@Test
	public void getAllRows(){
		//myhbase.getAllRows("info","basicinfo","name");
		myhbase.getAllRows("info","basicinfo","name");
	}
	@Test
	public void getRowsFromTo(){
		myhbase.getRowsFromTo("info","1","dd");
	}
	@Test
	public void getManyRows(){
		myhbase.getManyRows("info");
	}
	@Test
	public void singleColumnValueFilter(){
		myhbase.singleColumnValueFilter("info","educationbackground","primary","rixiang");
	}
	@Test
	public void singleColumnValueFilterByteArrayComparable(){
		myhbase.singleColumnValueFilterByteArrayComparable("info","basicinfo","name","row");
	}
	@Test
	public void singleColumnValueFilterSubStringComparable(){
		myhbase.singleColumnValueFilterSubStringComparable("info","basicinfo","name","row");
	}
	@Test
	public void familyFilter(){
		myhbase.familyFilter("info","basicinfo");
	}
	@Test
	public void columnPrefixFilter(){
		myhbase.columnPrefixFilter("info","name");
	}
	@Test
	public void rowFilter(){
		myhbase.rowFilter("info","1");
	}
	@Test
	public void valueFilter(){
		myhbase.valueFilter("info","rixiang");
	}
}
