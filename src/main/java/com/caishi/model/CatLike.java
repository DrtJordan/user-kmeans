package com.caishi.model;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * Created by apple on 15/9/14.
 */
public class CatLike implements Serializable, Comparable<CatLike> {
	private Integer catId;
	private double weight;

	public CatLike() {
	}

	public CatLike(Integer catId, double weight) {
		this.catId = catId;
		this.weight = weight;
	}

	public Integer getCatId() {
		return catId;
	}

	public void setCatId(Integer catId) {
		this.catId = catId;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}

	/**
	 * 逆序排列
	 *
	 * @param o
	 * @return
	 */
	@Override
	public int compareTo(CatLike o) {
		if (o == null) {
			return 0;
		}
		if (this.weight < o.weight) {
			return 1;
		} else if (this.weight > o.weight) {
			return -1;
		}
		return 0;
	}
}
