package com.dbms.cassandra.datastax.client;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;

public class CassandraWrite {

	private static Map<String,String> attributeMap = new LinkedHashMap<String, String>();


	public static void main(String[] args) {

		String student_id = "100953487";

		attributeMap.put("Nikhil", "Nayyar");

		insertAttributes(student_id, attributeMap);
	}

	/**
	 * Performs an insert into Cassandra database.
	 */
	public static void insertAttributes(String rowKey, Map<String, String> attributes) {

		try {

			for(Map.Entry<String, String> map : attributes.entrySet()) {				
				String gender = "male";				
				String cql = "INSERT INTO STUDENTS (STUDENT_ID, FIRST_NAME, LAST_NAME, GENDER) VALUES ('"+rowKey+"', '"+map.getKey()+"', '"+map.getValue()+"', '"+gender+"')";
				System.out.println(cql);

				Builder builder = Cluster.builder();
				builder.addContactPoint("localhost");

				builder.poolingOptions().setCoreConnectionsPerHost(
						HostDistance.LOCAL,
						builder.poolingOptions().getMaxConnectionsPerHost(HostDistance.LOCAL));

				Cluster cluster = builder
				.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
				.withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
				.build();

				StringBuilder s = new StringBuilder();
				Set<Host> allHosts = cluster.getMetadata().getAllHosts();
				for (Host h : allHosts) {
					s.append("[");
					s.append(h.getDatacenter());
					s.append("-");
					s.append(h.getRack());
					s.append("-");
					s.append(h.getAddress());
					s.append("]");
				}
				System.out.println("Cassandra Cluster: " + s.toString());

				Session session = cluster.connect("dbmsks");


				session.execute(cql);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
