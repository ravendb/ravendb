using System;

namespace Sparrow.Logging;

public interface IRavenLogManager
{
    IRavenLogger GetLogger(string name);

    event EventHandler<RavenLoggingConfigurationChangedEventArgs> ConfigurationChanged;

    void Shutdown();
}
