package net.ravendb.abstractions.logging;

public interface ILog {
  public boolean isDebugEnabled();

  public boolean isWarnEnabled();

  public void log(LogLevel logLevel, String msg);

  public void log(LogLevel logLevel, String msg, Exception exception);

  public void debug(String msg);

  public void debug(String msg, Object... args);

  public void debugException(String msg, Throwable e);

  public void error(String message, Object... args);

  public void errorException(String msg, Throwable e);

  public void fatalException(String message, Throwable e);

  public void info(String msg);

  public void info(String msg, Object... args);

  public void infoException(String msg, Throwable e);

  public void warn(String msg);

  public void warn(String msg, Object... args);

  public void warnException(String msg, Throwable e);

}
