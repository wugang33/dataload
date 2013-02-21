package com.ztesoft.cep.utils;

import java.io.File;
import java.io.FileFilter;
import java.util.regex.Pattern;

public class MyFileFilter implements FileFilter {
	public String fileregex_ = ".*\\.cmd";

	public MyFileFilter(String fileregex) {
		this.fileregex_ = fileregex;
	}

	@Override
	public boolean accept(File pathname) {
		return Pattern.matches(fileregex_, pathname.getName());
	}

}