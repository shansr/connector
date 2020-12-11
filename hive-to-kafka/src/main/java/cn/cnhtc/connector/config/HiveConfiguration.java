package cn.cnhtc.connector.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * @author ThinkPad
 */
@Configuration
public class HiveConfiguration {

	@Bean
	public DataSource dataSource(){
		String driverName = "org.apache.hive.jdbc.HiveDriver";
		String url = "jdbc:hive2://10.100.111.3:10000/default";
		String user = "name";
		String password = "password";
		HikariConfig config = new HikariConfig();
		config.setConnectionTestQuery("select 1");
		config.setDriverClassName(driverName);
		config.setJdbcUrl(url);
		config.setUsername(user);
		config.setPassword(password);
		config.setMaximumPoolSize(2);
		config.setMinimumIdle(2000);
		return new HikariDataSource(config);
	}

	@Bean
	public JdbcTemplate jdbcTemplate(DataSource dataSource){
		return new JdbcTemplate(dataSource);
	}

}
