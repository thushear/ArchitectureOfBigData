package com.github.thushear.spark.dao;


import com.github.thushear.spark.domain.SessionDetail;

/**
 * Session明细DAO接口
 * @author Administrator
 *
 */
public interface ISessionDetailDAO {

	/**
	 * 插入一条session明细数据
	 * @param sessionDetail
	 */
	void insert(SessionDetail sessionDetail);

}
