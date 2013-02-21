package com.ztesoft.cep.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoadConfig {
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof LoadConfig)) {
			return false;
		}
		LoadConfig other = (LoadConfig) obj;
		return this.eventtype == other.eventtype
				&& this.tableSchemaString.equals(other.tableSchemaString)
				&& this.cycle.equals(other.cycle)
				&& this.splitMode.ordinal() == other.splitMode.ordinal();
	}

	private int eventtype;
	private String tableSchemaString;
	private String cycle;

	static private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");
	static private SimpleDateFormat sdf_day = new SimpleDateFormat("yyyyMMdd");
	static private SimpleDateFormat sdf_month = new SimpleDateFormat("yyyyMM");
	static private SimpleDateFormat sdf_year = new SimpleDateFormat("yyyy");

	public enum SPLIT_DATA_MODE {
		DAY, MONTH, YEAR, NONE
	};

	private SPLIT_DATA_MODE splitMode = SPLIT_DATA_MODE.NONE;

	public int getEventtype() {
		return eventtype;
	}

	public void setEventtype(int eventtype) {
		this.eventtype = eventtype;
	}

	public String getTableSchemaString() {
		return tableSchemaString;
	}

	public void setTableSchemaString(String tableSchemaString) {
		this.tableSchemaString = tableSchemaString;
	}

	public String getCycle() {
		return cycle;
	}

	public void setCycle(String cycle) {
		this.cycle = cycle;
	}

	public SPLIT_DATA_MODE getSplitMode() {
		return splitMode;
	}

	public void setSplitMode(SPLIT_DATA_MODE splitMode) {
		this.splitMode = splitMode;
	}

	public TableSchema getTableSchema() {
		return tableSchema;
	}

	public void setTableSchema(TableSchema tableSchema) {
		this.tableSchema = tableSchema;
	}

	private TableSchema tableSchema;

	public String buildTableName(Date date) {
		String partstr = null;
		switch (splitMode) {
		case NONE:
			break;
		case DAY:
			partstr = sdf_day.format(date);
			break;
		case MONTH:
			partstr = sdf_month.format(date);
			break;
		case YEAR:
			partstr = sdf_year.format(date);
			break;
		default:
			break;
		}
		String retstr = this.tableSchema.tablename;
		if (this.cycle != null && !this.cycle.equals("") && !this.cycle.equals("0")) {
			retstr = retstr + "_" + this.cycle;
		}
		if (partstr != null) {
			retstr = retstr + "_" + partstr;
		}
		return retstr;
	}

	public String buildNowTableName() {
		String partstr = null;
		String dateStr = df.format(new Date());
		try {
			Date date = df.parse(dateStr);
			switch (splitMode) {
			case NONE:
				break;
			case DAY:
				partstr = sdf_day.format(date);
				break;
			case MONTH:
				partstr = sdf_month.format(date);
				break;
			case YEAR:
				partstr = sdf_year.format(date);
				break;
			default:
				break;
			}
		} catch (ParseException e) {
			// logger.error("ParseException", e);
			return "";
		}
		String retstr = this.tableSchema.tablename;
		if (this.cycle != null && !this.cycle.equals("") && !this.cycle.equals("0")) {
			retstr = retstr + "_" + this.cycle;
		}
		if (partstr != null) {
			retstr = retstr + "_" + partstr;
		}
		return retstr;
	}

}
