package net.ravendb.abstractions.logging;

import net.ravendb.abstractions.logging.providers.CommonsLoggingProvider;

public class LogManager {

  private static ILogManager currentLogManager;

  public static ILog getCurrentClassLogger() {
    return getLogger(Thread.currentThread().getStackTrace()[2].getClassName());
  }

  public static ILog getLogger(String name) {
    ILogManager logManager = getCurrentLogManager();
    if (logManager == null) {
      throw new IllegalStateException("Unable to find logger");
    }
    return logManager.getLogger(name);
  }

  private static ILogManager getCurrentLogManager() {
    if (currentLogManager != null) {
      return currentLogManager;
    }
    synchronized (LogManager.class) {
      if (currentLogManager == null) {
        currentLogManager = new CommonsLoggingProvider();
      }
    }
    return currentLogManager;
  }

}
