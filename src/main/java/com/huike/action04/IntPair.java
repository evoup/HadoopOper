package com.huike.action04;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 自己定义的key类应该实现WritableComparable接口
 */
public class IntPair implements WritableComparable<IntPair> {
	int first;// 第一个成员变量
	int second;// 第二个成员变量
	private static final Log LOG = LogFactory.getLog(IntPair.class);

	public void set(int left, int right) {
		first = left;
		second = right;
		LOG.info("[IntPair][first:"+ first +"][second:" + second +"]");
	}

	public int getFirst() {
		return first;
	}

	public int getSecond() {
		return second;
	}

	@Override
	// 反序列化，从流中的二进制转换成IntPair
	public void readFields(DataInput in) throws IOException {
		first = in.readInt();
		second = in.readInt();
		LOG.info("[readFields][first:"+ first +"][second:" + second +"]");
	}

	@Override
	// 序列化，将IntPair转化成使用流传送的二进制
	public void write(DataOutput out) throws IOException {
		LOG.info("[readFields][write][out.writeInt][first:" + first + "][second:" + second + "]");
		out.writeInt(first);
		out.writeInt(second);
	}

	@Override
	public int compareTo(IntPair o) {
		if (first != o.first) {
			if (first < o.first) {
				LOG.info("[compareTo][first:" + first + "][o.first:" + o.first + "][return -1]");
			} else {
				LOG.info("[compareTo][first:" + first + "][o.first:" + o.first + "][return 1]");
			}
			return first < o.first ? -1 : 1;
		} else if (second != o.second) {
			if (second < o.second) {
				LOG.info("[compareTo][second:" + second + "][o.second:" + o.second + "][return -1]");
			} else {
				LOG.info("[compareTo][second:" + second + "][o.second:" + o.second + "][return 1]");
			}
			return second < o.second ? -1 : 1;
		} else {
			LOG.info("[compareTo][both equal][ret 0]");
			return 0;
		}
	}

	@Override
	public int hashCode() {
		LOG.info("[hashCode][first:" + first + "][second:" + second +"][result:" +
				first * 157 + second + "]");
		return first * 157 + second;
	}

	@Override
	public boolean equals(Object right) {
		LOG.info("[equals][right:" + new Gson().toJson(right) + "]");
		if (right == null) {
			LOG.info("[equals][return false]");
		}
		if (this == right) {
			LOG.info("[equals][return true]");
		}
		if (right == null)
			return false;
		if (this == right)
			return true;
		if (right instanceof IntPair) {
			IntPair r = (IntPair) right;
			LOG.info("[equals][instanceof IntPair][r:" + new Gson().toJson(r) + "]");
			if (r.first == first && r.second == second) {
				LOG.info("[equals][instanceof IntPair][true]");
			} else {
				LOG.info("[equals][instanceof IntPair][false]");
			}
			return r.first == first && r.second == second;
		} else {
			LOG.info("[equals][not instanceof IntPair][return false]");
			return false;
		}
	}
}