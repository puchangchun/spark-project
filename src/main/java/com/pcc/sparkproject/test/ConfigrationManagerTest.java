package com.pcc.sparkproject.test;

import com.pcc.sparkproject.conf.ConfigrationManager;
import com.pcc.sparkproject.constant.Constants;

public class ConfigrationManagerTest {
	public ConfigrationManagerTest() {
		System.out.println("gou zao qi init");
	}

	{
		System.out.println("11111");
	}

	public static void main(String[] args) {
		try {
			Class.forName("com.pcc.sparkproject.test.ConfigrationManagerTest").newInstance();
			new  ConfigrationManagerTest();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		System.out.println(ConfigrationManager.getProperty(Constants.SPARK_LOCAL));
	}
}
