package com.ztesoft.cep.model;

import java.util.ArrayList;
import java.util.List;

/*
 * 这个类用来分析标准SQL 
 */

public class TableSchema {
	@Override
	public String toString() {
		String s = "create table " + this.tablename + "(\n";
		for (int i = 0; i < fields.size(); i++) {
			TableField tf = fields.get(i);
			if (tf != null) {
				if (i == fields.size() - 1) {
					s = s + " " + tf.name + " " + tf.type + "  " + tf.constants + ")\n";
				} else {
					s = s + " " + tf.name + " " + tf.type + " " + tf.constants + ",\n";
				}
			}
		}
		s += this.tabextend;
		return s;
	}

	public class TableField {
		public String name;
		public String type;
		public String constants;
	}

	public String tablename;
	public List<TableField> fields = new ArrayList<TableField>();
	public String tabextend = "";
	public String tableschema;

	public boolean analyze(String tableschema) {
		if (tableschema == null)
			return false;
		String SQL = tableschema.toLowerCase();
		this.tableschema = SQL;
		/*
		 * 从建表语句中得到所有的字段 首先把建表语句里面所有括号包含的东西去掉 然后就可以得到根据,号分隔 然后得到字段
		 */
		/*
		 * 有限状态机 state=0 start 0遇到括号变成4 4遇到非空白字符就到1 state=1 匹配最开始的(
		 * 也就是希望得到下一个字段的值 1遇到不可见字符变成2 1遇到括号变成3 同时把字段值存进去（存进去之前要与table
		 * constraint比较 如果相等 那么不存进去 同时代表匹配结束流程变成5） 1不可能遇到， state=2 匹配完了字段值
		 * 那么下一个希望得到的是下一个,2遇到，变成1,2遇到（变成3 2遇到） 流程结束变成5 state=3 1=》2 1得到了一个括号
		 * 那么他不会匹配下一个，号 3遇到）变成2 如果最后的流程不是5 那么失败
		 */
		int state = 7;
		String field = "";
		TableField tf = new TableField();
		for (int i = 0; i < SQL.length(); i++) {
			switch (state) {
			/*
			 * 后来添加 用来匹配表名 6是最开始的状态 7 是开始匹配表名的状态
			 */
			case 7: {
				char ch = SQL.charAt(i);
				if ('c' == ch) {
					field += SQL.charAt(i);
					state = 6;
				}
			}
				break;
			/*
			 * 6是开启匹配table 匹配了table 那么就需要匹配表名了
			 */
			case 6: {
				char ch = SQL.charAt(i);
				if (ch <= 0x20) {
					if (field.equals("table")) {
						state = 8;
					}
					field = "";
				} else {
					field += ch;
				}
			}
				break;
			/*
			 * 开始匹配表名
			 */
			case 8: {
				char ch = SQL.charAt(i);
				if (ch > 0x20) {
					field += ch;
					state = 9;
				}

			}
				break;
			case 9: {
				char ch = SQL.charAt(i);
				if (ch > 0x20 && ch != '(') {
					field += ch;
				}
				if (ch <= 0x20) {
					this.tablename = field;
					field = "";
					state = 0;
				}
				if (ch == '(') {
					this.tablename = field;
					field = "";
					state = 4;
				}
			}
				break;
			/*
			 * 开始匹配字段
			 */
			case 0: {
				if ('(' == SQL.charAt(i)) {
					state = 4;
				}
			}
				break;
			/*
			 * 匹配字段的值
			 */
			case 1: {
				char ch = SQL.charAt(i);
				if (ch <= 0x20)// 不可见字符
				{
					tf.name = field;
					fields.add(tf);
					field = "";
					state = 10;
				}// end if (ch <= 0x20
				else {
					field += ch;
				}
			}
				break;
			/*
			 * 开始匹配类型前面的空白
			 */
			case 10: {
				char ch = SQL.charAt(i);
				if (ch > 0x20) {
					field += ch;
					state = 2;
				}
			}
				break;
			/*
			 * 匹配完了字段名称 开始要匹配类型了
			 */

			case 2://
			{
				char ch = SQL.charAt(i);
				if (',' == ch) {// 类型匹配完了 constants为空
					state = 4;
					tf.type = field;
					tf.constants = "";
					tf = new TableField();
					field = "";
				}// end ','==ch
				else if ('(' == ch) {
					state = 3;
					field += ch;
				} else if (')' == ch) {
					tf.type = field;
					tf.constants = "";
					field = "";
					state = 5;
				} else if (ch > 0x20) {
					field += ch;
				} else if (ch <= 0x20) {
					/*
					 * 类型匹配完了 需要匹配constants
					 */
					tf.type = field;
					field = "";
					state = 11;
				}
			}

				break;
			case 11: {
				char ch = SQL.charAt(i);
				if (',' == ch) {
					tf.constants = field;
					tf = new TableField();
					field = "";
					state = 4;
				} else if (')' == ch) {
					tf.constants = "";
					field = "";
					state = 5;
				} else if ('(' == ch) {
					field += ch;
					state = 13;
				} else if (ch > 0x20) {
					field += ch;
					state = 12;
				}

			}
				break;
			case 12: {
				char ch = SQL.charAt(i);
				if ('(' == ch) {
					state = 13;
					field += ch;
				} else if (',' == ch) {
					tf.constants = field;
					tf = new TableField();
					field = "";
					state = 4;
				} else if (')' == ch) {
					tf.constants = field;
					field = "";
					state = 5;
				} else {
					field += ch;
				}
			}
				break;
			case 13: {
				char ch = SQL.charAt(i);
				if (')' == ch) {
					state = 12;
				}
				field += ch;
			}
				break;
			case 3:// 用来处理 字段后面的,号 字段后面的,号是在括号中的 所以直接无视括号中的，号就行了
			{
				char ch = SQL.charAt(i);
				if (')' == ch) {
					state = 2;
				}
				field += ch;
			}
				break;
			case 4:// 匹配第一个非空白字符
			{
				char ch = SQL.charAt(i);
				if (ch > ' ') {
					state = 1;
					field += ch;
				}
			}
				break;
			case 5:// 已经匹配完了 可以退出了
			{
				if (i == SQL.length() - 1) {
					field += SQL.charAt(i);
					this.tabextend = field;
				} else {
					field += SQL.charAt(i);
				}
			}
				break;
			default:
				break;
			}
		}
		return true;
	}

	public static void main(String argv[]) {
		// Date d = new Date();
		// System.out.println(d.getTime() % 6);
		TableSchema ts = new TableSchema();
		String s = "create table zxt2000.ST_EE_PagingFailed_5 (           "
				+ " BTIME                timestamp                      not null iq unique (255),                     "
				+ "HH					 integer                        not null,"
				+ "MI						integer                        not null,"
				+ "IMSI						varchar(64)                    not null iq unique (255),"
				+ "   TMSI                 varchar(64)                    null iq unique (255),"
				+ "   MSISDN               varchar(26)                    null iq unique (255),"
				+ "   LAC                  integer                        not null iq unique (255),"
				+ "   CI                   integer                        not null iq unique (255),"
				+ "   AREAID               integer                        not null,"
				+ "   vipGroupid           integer                        not null,"
				+ "    CALLTYPE             unsigned int                        not null,"
				+ "   FailedCount          integer                        null iq unique (255),"
				+ "   TotalCallCount       integer                        null iq unique (255))INTERVAL (numtodsinterval(5,'minute')) ";
		ts.analyze(s);
		// System.out.println(ts.toString());
		System.out.println(ts.tableschema);
	}
}
