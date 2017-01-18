package com.github.thushear.spark.dao.factory;


import com.github.thushear.spark.dao.*;
import com.github.thushear.spark.dao.impl.*;

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


  public static ITop10CategoryDAO getTop10CategoryDAO(){
    return new Top10CategoryDAOImpl();
  }
  public static ITop10SessionDAO getTop10SessionDAO() {
    return new Top10SessionDAOImpl();
  }

}
