package com.ztesoft.cep.config;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import com.ztesoft.cep.model.LoadConfig;

@SuppressWarnings("rawtypes")
class LoadConfigExtractor implements ResultSetExtractor {
	// public String toString() {
	// return "eventtype[" + eventtype + "]partgranularity["
	// + partgranularity + "]tableschema[" + tableschema + "]";
	// // + "]loadextend1[" + loadextend1 + "]loadextend2["
	// + loadextend2 + "]loadextend3[" + loadextend3 + "]";
	// }
	@Override
	public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
		List<LoadConfig> o = null;
		while (rs.next()) {
			if (o == null) {
				o = new ArrayList<LoadConfig>();
			}
			LoadConfig config = new LoadConfig();
			config.setEventtype(rs.getInt(1));
			config.setCycle(rs.getString(4));
			config.setTableSchemaString(rs.getString(3));
			int splitmodeint = rs.getInt(2);
			LoadConfig.SPLIT_DATA_MODE splitmode = LoadConfig.SPLIT_DATA_MODE.NONE;
			switch (splitmodeint) {
			case 0:
				splitmode = LoadConfig.SPLIT_DATA_MODE.NONE;
				break;
			case 1:
				splitmode = LoadConfig.SPLIT_DATA_MODE.DAY;
				break;
			case 2:
				splitmode = LoadConfig.SPLIT_DATA_MODE.MONTH;
				break;
			case 3:
				splitmode = LoadConfig.SPLIT_DATA_MODE.YEAR;
				break;
			default:
				splitmode = LoadConfig.SPLIT_DATA_MODE.NONE;
				break;
			}
			config.setSplitMode(splitmode);
			o.add(config);
		}
		return o;
	}
}