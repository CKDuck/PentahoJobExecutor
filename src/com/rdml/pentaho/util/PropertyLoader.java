package com.rdml.pentaho.util;

import java.util.ResourceBundle;

public class PropertyLoader {

	private static ResourceBundle properties = ResourceBundle.getBundle ("config");
	
	public static String getString(String key) {
		return properties.getString(key);
	}
}
