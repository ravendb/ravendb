package net.ravendb.abstractions.logging.providers;

import net.ravendb.abstractions.logging.ILog;
import net.ravendb.abstractions.logging.ILogManager;

import org.apache.commons.logging.LogFactory;


public class CommonsLoggingProvider implements ILogManager  {

  @Override
  public ILog getLogger(String name) {
    return new CommonsLoggingLogWrapper(LogFactory.getLog(name));
  }

}
