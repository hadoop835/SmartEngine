package com.alibaba.smart.framework.engine.starter;

import com.alibaba.smart.framework.engine.SmartEngine;
import com.alibaba.smart.framework.engine.configuration.ConfigurationOption;
import com.alibaba.smart.framework.engine.configuration.InstanceAccessor;
import com.alibaba.smart.framework.engine.configuration.ProcessEngineConfiguration;
import com.alibaba.smart.framework.engine.configuration.impl.DefaultProcessEngineConfiguration;
import com.alibaba.smart.framework.engine.configuration.impl.DefaultSmartEngine;
import com.alibaba.smart.framework.engine.exception.EngineException;
import com.alibaba.smart.framework.engine.service.command.ExecutionCommandService;
import com.alibaba.smart.framework.engine.service.command.ProcessCommandService;
import com.alibaba.smart.framework.engine.service.command.RepositoryCommandService;
import com.alibaba.smart.framework.engine.service.query.ExecutionQueryService;
import com.alibaba.smart.framework.engine.service.query.ProcessQueryService;
import com.alibaba.smart.framework.engine.service.query.RepositoryQueryService;
import com.alibaba.smart.framework.engine.util.ClassUtil;
import com.alibaba.smart.framework.engine.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 自动装配服务编排
 * @author chenzhh
 */
@Configuration
@ConditionalOnClass({SmartEngine.class})
public class SmartEngineAutoConfiguration implements ApplicationContextAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(SmartEngineAutoConfiguration.class);
   private  ApplicationContext applicationContext;

   private ConfigurableEnvironment configurableEnvironment;
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
          this.applicationContext = applicationContext;
    }

    @Autowired
    public SmartEngineAutoConfiguration(ConfigurableEnvironment configurableEnvironment) {
        this.configurableEnvironment = configurableEnvironment;
    }

    @Bean
    @ConditionalOnMissingBean
    public SmartEngine constructSmartEngine() {
        ProcessEngineConfiguration processEngineConfiguration = new DefaultProcessEngineConfiguration();
        /**
         * service.orchestration.executor.num=4
         * 指定线程池,多线程fork
         */
        String executornum = configurableEnvironment.getProperty("service.orchestration.executor.num","4");
        LOGGER.info("根据实际情况在yaml文件中配置线程数量,默认:service.orchestration.executor.num=4");
        processEngineConfiguration.setExecutorService(newFixedThreadPool(Integer.parseInt(executornum)));
        /**
         * 服务编排场景,必须要手动开启这个开关
         */
        processEngineConfiguration.getOptionContainer().put(ConfigurationOption.SERVICE_ORCHESTRATION_OPTION);
        processEngineConfiguration.setInstanceAccessor(new SmartEngineAutoConfiguration.CustomInstanceAccessService());
        SmartEngine smartEngine = new DefaultSmartEngine();
        smartEngine.init(processEngineConfiguration);
        String location = configurableEnvironment.getProperty("service.orchestration.location","true");
        LOGGER.info("根据实际情况在yaml文件中配置是否读取本地文件,如果集成数据库，修改为：false,默认:service.orchestration.location=true");
        if(Boolean.parseBoolean(location)){
            this.deployProcessDefinition(smartEngine);
        }
        return smartEngine;
    }

    @Bean
    @ConditionalOnMissingBean
    public  RepositoryCommandService  repositoryCommandService(){ return constructSmartEngine().getRepositoryCommandService();
    }
    @Bean
    @ConditionalOnMissingBean
    public ProcessCommandService processCommandService(){
        return constructSmartEngine().getProcessCommandService();
    }
    @Bean
    @ConditionalOnMissingBean
    public ProcessQueryService processQueryService(){
        return constructSmartEngine().getProcessQueryService();
    }
    @Bean
    @ConditionalOnMissingBean
    public ExecutionQueryService executionQueryService(){
        return constructSmartEngine().getExecutionQueryService();
    }
    @Bean
    @ConditionalOnMissingBean
    protected ExecutionCommandService executionCommandService(){ return constructSmartEngine().getExecutionCommandService();
    }
    @Bean
    @ConditionalOnMissingBean
    protected RepositoryQueryService repositoryQueryService(){ return constructSmartEngine().getRepositoryQueryService();
    }

    /**
     * 部署文件目录
     * @param smartEngine
     */
    private void deployProcessDefinition(SmartEngine smartEngine) {
        RepositoryCommandService repositoryCommandService = smartEngine.getRepositoryCommandService();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        try {
            Resource[] resources = resolver.getResources("classpath*:/service-orchestration/*.xml");
            for(Resource resource : resources){
                InputStream inputStream = resource.getInputStream();
                repositoryCommandService.deploy(inputStream);
                IOUtil.closeQuietly(inputStream);
            }
        } catch (Exception ex) {
            throw new EngineException(ex);
        }
    }

    /**
     * 线程池
     * @param nThreads
     * @return
     */
    private static ExecutorService newFixedThreadPool(int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    /**
     * spring 容器上线文取对象
     */
    private class CustomInstanceAccessService implements InstanceAccessor {
        private CustomInstanceAccessService() {
        }

        @Override
        public Object access(String classNameOrBeanName) {
            try {
                Class clazz = ClassUtil.getContextClassLoader().loadClass(classNameOrBeanName);
                return  applicationContext.getBean(clazz);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
