package flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.List;
import java.util.Map;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import entity.LoginEvent;
import entity.LoginWarning;

public class BruteForceAttack {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<LoginEvent> loginEventDataStream = env.fromElements(new LoginEvent("bob", "192.168.0.1", "fail"),
				new LoginEvent("bob", "192.168.0.1", "fail"), new LoginEvent("bob", "192.168.0.1", "fail"),
				new LoginEvent("bob", "192.168.0.1", "fail"), new LoginEvent("bob", "192.168.0.1", "fail"),
				new LoginEvent("alice", "127.0.0.1", "fail"), new LoginEvent("alice", "127.0.0.1", "fail"),
				new LoginEvent("alice", "127.0.0.1", "fail"), new LoginEvent("alice", "127.0.0.1", "fail"),
				new LoginEvent("alice", "127.0.0.1", "fail"), new LoginEvent("alice", "127.0.0.1", "success"),
				new LoginEvent("bob", "192.168.0.1", "fail"), new LoginEvent("bob", "192.168.0.1", "fail"),
				new LoginEvent("bob", "192.168.0.1", "fail"), new LoginEvent("bob", "192.168.0.1", "fail"),
				new LoginEvent("bob", "192.168.0.1", "fail"), new LoginEvent("bob", "192.168.0.1", "success"),
				new LoginEvent("charlie", "0.0.0.0", "fail"), new LoginEvent("charlie", "0.0.0.1", "fail"),
				new LoginEvent("charlie", "0.0.0.2", "fail"), new LoginEvent("charlie", "0.0.0.3", "fail"),
				new LoginEvent("charlie", "0.0.0.4", "fail"), new LoginEvent("charlie", "0.0.0.5", "fail"),
				new LoginEvent("charlie", "0.0.0.6", "fail"), new LoginEvent("charlie", "0.0.0.7", "fail"),
				new LoginEvent("charlie", "0.0.0.8", "fail"), new LoginEvent("charlie", "0.0.0.9", "fail"),
				new LoginEvent("charlie", "0.0.0.10", "success"));

		Pattern<LoginEvent, LoginEvent> bruteForcePattern = Pattern.<LoginEvent>begin("More than 10 failed logins")
				.where(new SimpleCondition<LoginEvent>() {
				
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(LoginEvent event) throws Exception {
						return event.getType().equals("fail");
					}
				}).times(10).next("Successful login").where(new SimpleCondition<LoginEvent>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(LoginEvent event) throws Exception {
						return event.getType().equals("success");
					}
				}).within(Time.minutes(10));

		DataStream<LoginWarning> loginWarningDataStream = CEP
				.pattern(loginEventDataStream.keyBy(LoginEvent -> LoginEvent.getUserId() + LoginEvent.getIp()),
						bruteForcePattern)
				.select(new PatternSelectFunction<LoginEvent, LoginWarning>() {
					private static final long serialVersionUID = 1L;

					@Override
					public LoginWarning select(Map<String, List<LoginEvent>> map) throws Exception {
						LoginEvent bruteForcedLogin = map.get("Successful login").get(0);
						return new LoginWarning(bruteForcedLogin.getUserId(), bruteForcedLogin.getType(),
								bruteForcedLogin.getIp());
					}
				});

		loginWarningDataStream.print();

		env.execute();
	}

}
