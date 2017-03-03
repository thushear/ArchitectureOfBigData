package com.github.thushear.spark.dao.impl;


import com.github.thushear.spark.dao.ISessionDetailDAO;
import com.github.thushear.spark.domain.SessionDetail;
import com.github.thushear.spark.jdbc.JDBCHelper;

/**
 * session明细DAO实现类
 * @author Administrator
 *
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {

	/**
	 * 插入一条session明细数据
	 * @param sessionDetail
	 */
	public void insert(SessionDetail sessionDetail) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

		Object[] params = new Object[]{sessionDetail.getTaskid(),
				sessionDetail.getUserid(),
				sessionDetail.getSessionid(),
				sessionDetail.getPageid(),
				sessionDetail.getActionTime(),
				sessionDetail.getSearchKeyword(),
				sessionDetail.getClickCategoryId(),
				sessionDetail.getClickProductId(),
				sessionDetail.getOrderCategoryIds(),
				sessionDetail.getOrderProductIds(),
				sessionDetail.getPayCategoryIds(),
				sessionDetail.getPayProductIds()};

		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
