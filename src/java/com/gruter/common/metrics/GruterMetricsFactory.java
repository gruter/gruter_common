package com.gruter.common.metrics;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author jindolk
 *
 */
public class GruterMetricsFactory {
  public static final Log LOG =
    LogFactory.getLog(GruterMetricsFactory.class.getName());
  
  private static final String PROPERTIES_FILE = "/gruter-metrics.properties";
  
  private static GruterMetricsFactory factory;

  //key -> value
  private Map<String, String> metricsAttributes = new HashMap<String, String>();

  private GruterMetricsFactory() {
  }
  
  public static synchronized GruterMetricsFactory getFactory() {
    if(factory == null) {
      factory = new GruterMetricsFactory();
      try {
        factory.setAttributes();
      } catch (IOException e) {
        LOG.error(e);
      }
    }
    
    return factory;
  }
  
  public String getAttribute(String key) {
    return metricsAttributes.get(key);
  }
  
  public String[] getAttributeNames() {
    String[] result = new String[metricsAttributes.size()];
    int i = 0;
    // for (String attributeName : attributeMap.keySet()) {
    Iterator it = metricsAttributes.keySet().iterator();
    while (it.hasNext()) {
      result[i++] = (String) it.next();
    }
    return result;
  }
  
  public GruterMetricsContext getContext(String metricsName, String contextName) {
    String contextClassName = metricsAttributes.get(contextName + ".context");

    if(contextClassName == null) {
      LOG.warn("No context class info for " + contextName + 
          ", check [" + contextName + ".context] property in conf/gruter-metrics.properties");
      contextClassName = DummyMetricContext.class.getName();
    }
    GruterMetricsContext context;
    
    try {
      context = (GruterMetricsContext)Class.forName(contextClassName).newInstance(); 
    } catch (Exception e) {
      LOG.warn("Can't make context object for " + contextName + 
          ", check [" + contextName + ".context] property in conf/cloudata-metrics.properties, " + e.getMessage());
      context = new DummyMetricContext(); 
    }

    LOG.info(metricsName + "," + contextName + " load:" + context.getClass());
    context.setMetricsName(metricsName, contextName);
    if(context.isMonitoring()) {
      context.startMonitoring();
    }
    
    return context;
  }
  
  public ObjectName registerMBean(final String serviceName,
      final String nameName, final Object theMbean) {
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = getMBeanName(serviceName, nameName);
    try {
      mbs.registerMBean(theMbean, name);
      return name;
    } catch (InstanceAlreadyExistsException ie) {
      // Ignore if instance already exists
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public void unregisterMBean(ObjectName mbeanName) {
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    if (mbeanName == null)
      return;
    try {
      mbs.unregisterMBean(mbeanName);
    } catch (InstanceNotFoundException e) {
      // ignore
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private ObjectName getMBeanName(final String serviceName,
      final String nameName) {
    ObjectName name = null;
    try {
      name = new ObjectName("org.cloudata:" + "service=" + serviceName + ",name="
          + nameName);
    } catch (MalformedObjectNameException e) {
      e.printStackTrace();
    }
    return name;
  }
  
  private void setAttributes() throws IOException {
    InputStream is = getClass().getResourceAsStream(PROPERTIES_FILE);
    if (is != null) {
      try {
        Properties properties = new Properties();
        properties.load(is);
        Iterator it = properties.keySet().iterator();
        while (it.hasNext()) {
          String propertyName = (String) it.next();
          String propertyValue = properties.getProperty(propertyName);
          metricsAttributes.put(propertyName, propertyValue);
        }
      } finally {
        is.close();
      }
    }
  }
}
