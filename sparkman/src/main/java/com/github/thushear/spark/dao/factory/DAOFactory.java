package com.github.thushear.spark.dao.factory;


import com.github.thushear.spark.dao.ISessionAggrStatDAO;
import com.github.thushear.spark.dao.ISessionDetailDAO;
import com.github.thushear.spark.dao.ISessionRandomExtractDAO;
import com.github.thushear.spark.dao.ITaskDAO;
import com.github.thushear.spark.dao.impl.SessionAggrStatDAOImpl;
import com.github.thushear.spark.dao.impl.SessionDetailDAOImpl;
import com.github.thushear.spark.dao.impl.SessionRandomExtractDAOImpl;
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


	public static ISessionAggrStatDAO getSessionAggrStatDAO(){
		return new SessionAggrStatDAOImpl();
	}


  public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
    return new SessionRandomExtractDAOImpl();
  }


  public static ISessionDetailDAO getSessionDetailDAO(){
    return new SessionDetailDAOImpl();

  }

}
