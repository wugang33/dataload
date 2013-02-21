package com.ztesoft.cep.config;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.ztesoft.cep.model.LoadTask;

@SuppressWarnings("rawtypes")
public class LoadTaskExtractor implements ResultSetExtractor {
	@Override
	public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
		List<LoadTask> tasks = null;
		// select
		// id,eventtype,contentTimestamp,destFilePath,destFileSize,state,taskGenTime,taskLastExecStartTime,taskLastExecEndTime,destTablename
		while (rs.next()) {
			if (tasks == null) {
				tasks = new ArrayList<LoadTask>();
			}
			LoadTask task = new LoadTask();
			task.setId(rs.getLong(1));
			task.setEventtype(rs.getInt(2));
			task.setContentTimestamp(rs.getDate(3));
			task.setDestFilePath(rs.getString(4));
			task.setDestFileSize(rs.getLong(5));
			task.setState(rs.getInt(6));
			task.setTaskGenTime(rs.getDate(7));
			task.setTaskLastExecStartTime(rs.getDate(8));
			task.setTaskLastExecStartTime(rs.getDate(9));
			task.setDestTablename(rs.getString(10));
			System.out.println(rs.getLong(1) + rs.getString(4) + rs.getDate(7));
			tasks.add(task);
		}
		// TODO Auto-generated method stub
		return tasks;
	}

}
