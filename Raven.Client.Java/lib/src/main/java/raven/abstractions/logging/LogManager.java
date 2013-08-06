package raven.abstractions.logging;

import raven.abstractions.logging.providers.CommonsLoggingProvider;

public class LogManager {

  private static ILogManager currentLogManager;

  public static ILog getCurrentClassLogger() {
    return getLogger(Thread.currentThread().getStackTrace()[1].getClassName());
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
    } else {
      synchronized (LogManager.class) {
        if (currentLogManager == null) {
          currentLogManager = new CommonsLoggingProvider();
        }
      }
      return currentLogManager;
    }
  }

}
