package org.apache.geode.connectors.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class JDBCConfiguration {

  private static final String DRIVER = "driver";

  private static final String URL = "url";

  private static final String USER = "user";

  private static final String PASSWORD = "password";

  private static final List<String> knownProperties =
      Collections.unmodifiableList(Arrays.asList(DRIVER, URL, USER, PASSWORD));

  private static final List<String> requiredProperties =
      Collections.unmodifiableList(Arrays.asList(DRIVER, URL));

  private final String driver;

  private String url;

  JDBCConfiguration(Properties configProps) {
    validateKnownProperties(configProps);
    validateRequiredProperties(configProps);
    this.driver = configProps.getProperty(DRIVER);
    this.url = configProps.getProperty(URL);
  }

  private void validateKnownProperties(Properties configProps) {
    Set<Object> keys = new HashSet<>(configProps.keySet());
    keys.removeAll(knownProperties);
    if (!keys.isEmpty()) {
      throw new IllegalArgumentException("unknown properties: " + keys);
    }
  }

  private void validateRequiredProperties(Properties configProps) {
    List<String> reqKeys = new ArrayList<>(requiredProperties);
    reqKeys.removeAll(configProps.keySet());
    if (!reqKeys.isEmpty()) {
      Collections.sort(reqKeys);
      throw new IllegalArgumentException("missing required properties: " + reqKeys);
    }
  }

  public String getDriver() {
    return this.driver;
  }

  public String getURL() {
    return this.url;
  }

}
