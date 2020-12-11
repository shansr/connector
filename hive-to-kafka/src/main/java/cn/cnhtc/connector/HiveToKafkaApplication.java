package cn.cnhtc.connector;

import com.alibaba.fastjson.JSONObject;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ThinkPad
 */
@SpringBootApplication
public class HiveToKafkaApplication implements CommandLineRunner {
	private final JdbcTemplate jdbcTemplate;
	private final KafkaTemplate<String, String> kafkaTemplate;

	public HiveToKafkaApplication(JdbcTemplate jdbcTemplate, KafkaTemplate<String, String> kafkaTemplate) {
		this.jdbcTemplate = jdbcTemplate;
		this.kafkaTemplate = kafkaTemplate;
	}
	public static void main(String[] args) {
		SpringApplication.run(HiveToKafkaApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		String tableName = "obd_data_201209";
		jdbcTemplate.query("select * from " + tableName, resultSet -> {
			int keyIndex = 0;
			int columnCount = resultSet.getMetaData().getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				if (resultSet.getMetaData().getColumnName(i).contains("sbh")) {
					keyIndex = i;
					break;
				}
			}
			if (keyIndex > 0) {
				// 从resultSet 中获取 key(对应sbh)
				String key = resultSet.getString(resultSet.getMetaData().getColumnName(keyIndex));

				Map<String, Object> map = new HashMap<>();
				for (int i = 1; i <= columnCount; i++) {
					map.put(resultSet.getMetaData().getColumnName(i), resultSet.getObject(resultSet.getMetaData().getColumnName(i)));
				}
				String jsonString = JSONObject.toJSONString(map);
				kafkaTemplate.send("clw_vehicleV6", key, jsonString);
			}
		});
	}
}
