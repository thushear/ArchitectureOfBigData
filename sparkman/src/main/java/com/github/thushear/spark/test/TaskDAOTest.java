package com.github.thushear.spark.test;


import com.github.thushear.spark.dao.ITaskDAO;
import com.github.thushear.spark.dao.factory.DAOFactory;
import com.github.thushear.spark.domain.Task;

/**
 * 任务管理DAO测试类
 * @author Administrator
 *
 */
public class TaskDAOTest {

	public static void main(String[] args) {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(2);
		System.out.println(task.getTaskName());
	}

}
