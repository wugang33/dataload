package com.ztesoft.cep;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import com.ztesoft.cep.model.LoadTask;
import com.ztesoft.cep.utils.FileUtils;

public class ArchiveThread implements Runnable {

	public List<LoadTask> comm_queue = new ArrayList<LoadTask>();
	public boolean isalive = true;
	public LoadMaster loadmaster = null;
	public Integer queuelock = new Integer(0);
	public Logger logger = Logger.getLogger(ArchiveThread.class.getName());

	public ArchiveThread(LoadMaster lm) {
		this.loadmaster = lm;
	}

	public void push(LoadTask task) {
		synchronized (queuelock) {
			comm_queue.add(task);
		}
	}

	public void pushAll(List<LoadTask> tasks) {
		synchronized (queuelock) {
			comm_queue.addAll(tasks);
		}
	}

	public LoadTask pop() {
		synchronized (queuelock) {
			if (!comm_queue.isEmpty())
				return comm_queue.remove(0);
			return null;
		}
	}

	public List<LoadTask> popAll() {
		List<LoadTask> tasks = null;
		synchronized (queuelock) {
			if (!comm_queue.isEmpty()) {
				tasks = new ArrayList<LoadTask>();
				tasks.addAll(comm_queue);
				comm_queue.clear();
			}
			return tasks;
		}
	}

	@Override
	public void run() {
		while (isalive) {
			try {
				List<LoadTask> tasks = popAll();
				// logger.info("  the archive quene size is  "+task.getDestTablename());
				if (tasks == null || tasks.isEmpty()) {
					try {
						Thread.sleep(5000);
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					}
					continue;
				}
				List<LoadTask> newTasks = new ArrayList<LoadTask>();
				List<LoadTask> oldTasks = new ArrayList<LoadTask>();
				for (LoadTask task : tasks) {
					if (task.isNewTask()) {
						newTasks.add(task);
						// delete the command file
						if (task.getCommadnFile().delete() == false) {
							logger.error("delete command file "
									+ task.getCommadnFile().getAbsolutePath() + " failure");
						}
					} else {
						oldTasks.add(task);
					}
					if (FileUtils.isExtension(task.getDestFilePath(), "pipe4cep")) {
						logger.debug("delete pipe4cep");
						FileUtils.openDeleteClose(task.getDestFilePath());
					} else {
						if (task.getState() == LoadTask.TASKSTATE.SUCCESSFUL) {
							if (LoadMaster.instance().config.getDataLoadsucc().equals("delete")) {
								FileUtils.deleteCepDataFile(task.getDestFilePath());
							} else {
								logger.info("cep data not delete!");
							}
						} else {
							if (LoadMaster.instance().config.getDataLoadFailure().equals("delete")) {
								FileUtils.deleteCepDataFile(task.getDestFilePath());
							}
						}// end else
					}
				}
				LoadMaster.instance().dbServices.asynTaskArchive(newTasks, oldTasks);
			} catch (Exception e) {
				logger.error("archive exception must not here", e);
			}
		}// end while

	}
}