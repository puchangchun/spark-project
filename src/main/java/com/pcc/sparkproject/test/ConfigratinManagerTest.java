package com.pcc.sparkproject.test;

import com.pcc.sparkproject.conf.ConfigrationManager;
import com.pcc.sparkproject.constant.Constants;

public class ConfigratinManagerTest {
	public static void main(String[] args) {
		System.out.println(ConfigrationManager.getProperty(Constants.SPARK_LOCAL));
	}
}
