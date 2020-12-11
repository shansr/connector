package cn.cnhtc.connector;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;

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
//			// todo
//
//			// 从resultSet 中获取 key(对应sbbh)
//			String key = "";
//			// 将resultSet 转换成 Json 字符串
//
//			// todo
//			String value = "";
//			// 以设备号为key, 转换后的String 为 value 发送给 kafka
//			kafkaTemplate.send(key, value);
		});
	}
}
