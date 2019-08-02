package com.chenyiwenglish.hamq.spring.boot.autoconfigure;

import com.chenyiwenglish.hamq.MessageIdGenerator;
import com.chenyiwenglish.hamq.impl.SimpleIdGenerator;
import com.chenyiwenglish.hamq.mapper.MessageQueueInfoMapper;
import com.chenyiwenglish.hamq.mapper.MessageQueueMapper;
import com.chenyiwenglish.hamq.spring.boot.autoconfigure.consumer.MessageQueueConsumerRegistrar;
import com.chenyiwenglish.hamq.spring.boot.autoconfigure.producer.MessageQueueProducerRegistrar;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.mapper.MapperFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import javax.sql.DataSource;
import java.io.IOException;

@Configuration
@ConditionalOnProperty(
        prefix = "chenyiwenglish.hamq.autoconfigure",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
@Import({MessageQueueConsumerRegistrar.class, MessageQueueProducerRegistrar.class})
@AutoConfigureAfter(HamqConfigAutoConfiguration.class)
public class HamqAutoConfiguration {

    @Bean
    public HamqConfigService hamqConfigService(HamqProperties properties, Environment environment) {
        return new HamqConfigService(properties, environment);
    }

    @Bean("hamqDataSource")
    @Qualifier("hamqDataSource")
    @ConfigurationProperties(prefix = "chenyiwenglish.hamq.datasource")
    public DataSource hamqDataSource() {
        HikariDataSource hikariDataSource = DataSourceBuilder.create().type(HikariDataSource.class).build();
        return hikariDataSource;
    }

    @Bean(name = "hamqSqlSessionFactory")
    @Qualifier("hamqSqlSessionFactory")
    public SqlSessionFactoryBean hamqSqlSessionFactoryBean(@Qualifier("hamqDataSource") DataSource dataSource)
            throws IOException {
        SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
        sqlSessionFactoryBean.setDataSource(dataSource);
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("classpath*:chenyiwenglish/hamq/mapper/*.xml");
        sqlSessionFactoryBean.setMapperLocations(resources);
        return sqlSessionFactoryBean;
    }

    @Bean(name = "messageQueueMapper")
    @SuppressWarnings("unchecked")
    public MapperFactoryBean messageQueueMapper(
            @Qualifier("hamqSqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
        MapperFactoryBean mapperFactoryBean = new MapperFactoryBean();
        mapperFactoryBean.setMapperInterface(MessageQueueMapper.class);
        mapperFactoryBean.setSqlSessionFactory(sqlSessionFactory);
        return mapperFactoryBean;
    }

    @Bean(name = "messageQueueInfoMapper")
    @SuppressWarnings("unchecked")
    public MapperFactoryBean messageQueueInfoMapper(
            @Qualifier("hamqSqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
        MapperFactoryBean mapperFactoryBean = new MapperFactoryBean();
        mapperFactoryBean.setMapperInterface(MessageQueueInfoMapper.class);
        mapperFactoryBean.setSqlSessionFactory(sqlSessionFactory);
        return mapperFactoryBean;
    }

    @Bean(name = "messageIdGenerator")
    public MessageIdGenerator messageIdGenerator() {
        MessageIdGenerator messageIdGenerator = new SimpleIdGenerator();
        return messageIdGenerator;
    }
}
