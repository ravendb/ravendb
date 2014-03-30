package net.ravendb.abstractions.logging.providers;

import net.ravendb.abstractions.logging.ILog;
import net.ravendb.abstractions.logging.LogLevel;

import org.apache.commons.logging.Log;


public class CommonsLoggingLogWrapper implements ILog {

  private Log log;

  public CommonsLoggingLogWrapper(Log log) {
    this.log = log;
  }

  @Override
  public boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }

  @Override
  public boolean isWarnEnabled() {
    return log.isWarnEnabled();
  }

  @Override
  public void log(LogLevel logLevel, String msg) {
    switch(logLevel) {
    case TRACE:
      debug(msg);
      break;
    case DEBUG:
      debug(msg);
      break;
    case INFO:
      info(msg);
      break;
    case WARN:
      warn(msg);
      break;
    case ERROR:
      error(msg);
      break;
    case FATAL:
      fatalException(msg, null);
      break;
    }
  }

  @Override
  public void log(LogLevel logLevel, String msg, Exception e) {
    switch(logLevel) {
    case TRACE:
      debugException(msg, e);
      break;
    case DEBUG:
      debugException(msg, e);
      break;
    case INFO:
      infoException(msg, e);
      break;
    case WARN:
      warnException(msg, e);
      break;
    case ERROR:
      errorException(msg, e);
      break;
    case FATAL:
      fatalException(msg, e);
      break;
    }
  }

  @Override
  public void debug(String msg) {
    log.debug(msg);
  }

  @Override
  public void debug(String msg, Object... args) {
    log.debug(String.format(msg, args));
  }

  @Override
  public void debugException(String msg, Throwable e) {
    log.debug(msg, e);
  }

  @Override
  public void error(String message, Object... args) {
    log.error(String.format(message, args));
  }

  @Override
  public void errorException(String msg, Throwable e) {
    log.error(msg, e);
  }

  @Override
  public void fatalException(String message, Throwable e) {
   log.fatal(message, e);
  }

  @Override
  public void info(String msg) {
    log.info(msg);
  }

  @Override
  public void info(String msg, Object... args) {
    log.info(String.format(msg, args));
  }

  @Override
  public void infoException(String msg, Throwable e) {
    log.info(msg, e);
  }

  @Override
  public void warn(String msg) {
    log.warn(msg);
  }

  @Override
  public void warn(String msg, Object... args) {
    log.warn(String.format(msg, args));
  }

  @Override
  public void warnException(String msg, Throwable e) {
    log.warn(msg, e);
  }

}
