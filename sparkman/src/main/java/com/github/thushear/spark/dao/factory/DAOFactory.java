package com.github.thushear.spark.dao.factory;


import com.github.thushear.spark.dao.ITaskDAO;
import com.github.thushear.spark.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {


	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}


}
