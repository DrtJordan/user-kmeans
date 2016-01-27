package com.caishi.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.io.Serializable;
import java.util.List;

/**
 * Created by apple on 15/9/14.
 * update by YMY 15/11/13
 */
public class UserCatLike implements Serializable {

	@JSONField(name="_id")
	private String id;
	private List<CatLike> catLikes;
	private String createTime;

	@JsonIgnore
	private int topN;// 保留topn 个 元素 如果为0 则默认全部保留

	public UserCatLike() {
	}

	public UserCatLike(String id, List<CatLike> catLikes) {
		this.id = id;
		this.catLikes = catLikes;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<CatLike> getCatLikes() {
		return catLikes;
	}

	public void setCatLikes(List<CatLike> catLikes) {
		this.catLikes = catLikes;
	}

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public int getTopN() {
		return topN;
	}

	public void setTopN(int topN) {
		this.topN = topN;
	}

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}

}
