package net.ravendb.abstractions.data;

import java.util.HashMap;
import java.util.Map;

public class DatabaseDocument {
  private String id;
  private Map<String, String> settings;
  private Map<String, String> securedSettings;
  private boolean disabled;
  /**
   * @return the id
   */
  public String getId() {
    return id;
  }
  /**
   * @param id the id to set
   */
  public void setId(String id) {
    this.id = id;
  }
  /**
   * @return the settings
   */
  public Map<String, String> getSettings() {
    return settings;
  }
  /**
   * @param settings the settings to set
   */
  public void setSettings(Map<String, String> settings) {
    this.settings = settings;
  }
  /**
   * @return the securedSettings
   */
  public Map<String, String> getSecuredSettings() {
    return securedSettings;
  }
  /**
   * @param securedSettings the securedSettings to set
   */
  public void setSecuredSettings(Map<String, String> securedSettings) {
    this.securedSettings = securedSettings;
  }
  /**
   * @return the disabled
   */
  public boolean isDisabled() {
    return disabled;
  }
  /**
   * @param disabled the disabled to set
   */
  public void setDisabled(boolean disabled) {
    this.disabled = disabled;
  }


  public DatabaseDocument() {
    settings = new HashMap<>();
    securedSettings = new HashMap<>();
  }
}
