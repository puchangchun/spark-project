package com.pcc.sparkproject.test;

import com.pcc.sparkproject.conf.ConfigrationManager;
import com.pcc.sparkproject.constant.Constant;

public class ConfigratinManagerTest {
	public static void main(String[] args) {
		System.out.println(ConfigrationManager.getProperty(Constant.SPARK_LOCAL));
	}
}
