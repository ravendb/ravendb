package raven.abstractions.logging.providers;

import org.apache.commons.logging.LogFactory;

import raven.abstractions.logging.ILog;
import raven.abstractions.logging.ILogManager;

public class CommonsLoggingProvider implements ILogManager  {

  @Override
  public ILog getLogger(String name) {
    return new CommonsLoggingLogWrapper(LogFactory.getLog(name));
  }

}
