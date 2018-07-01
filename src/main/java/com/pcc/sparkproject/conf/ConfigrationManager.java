package com.pcc.sparkproject.conf;

import java.io.IOException;
import java.util.Properties;

/**
 * 配置管理组件
 * 
 * @author 99653
 *
 */
public class ConfigrationManager {

	private static Properties prop = new Properties();

	/**
	 * 静态代码块 Java中，每一个类第一次使用的时候，就会被Java虚拟机（JVM）中的类加载器，去从磁盘上的.class文件中
	 * 加载出来，然后为每个类都会构建一个Class对象，就代表了这个类
	 * 
	 * 每个类在第一次加载的时候，都会进行自身的初始化，那么类初始化的时候，会执行哪些操作的呢？ 就由每个类内部的static
	 * {}构成的静态代码块决定，我们自己可以在类中开发静态代码块 类第一次使用的时候，就会加载，加载的时候，就会初始化类，初始化类的时候就会执行类的静态代码块
	 * 
	 * 因此，对于我们的配置管理组件，就在静态代码块中，编写读取配置文件的代码
	 * 这样的话，第一次外界代码调用这个ConfigurationManager类的静态方法的时候，就会加载配置文件中的数据
	 * 
	 * 而且，放在静态代码块中，还有一个好处，就是类的初始化在整个JVM生命周期内，有且仅有一次，也就是说
	 * 配置文件只会加载一次，然后以后就是重复使用，效率比较高；不用反复加载多次
	 */
	static {

		try {
			prop.load(ConfigrationManager.class.getClassLoader().getResourceAsStream("my.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取指定jvm配置文件的的value
	 * 
	 * @param key
	 *            指定的key
	 * @return key对应的valu e
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}

	/**
	 * 获取整数
	 * 
	 * @param key
	 * @return
	 */
	public static Integer getInteger(String key) {

		return Integer.parseInt(getProperty(key));

	}
	
	/**
	 * 获取boolean
	 * @param key
	 * @return
	 */
	public static Boolean getBoolean(String key) {
		
		return Boolean.valueOf(getProperty(key));
	}
}
