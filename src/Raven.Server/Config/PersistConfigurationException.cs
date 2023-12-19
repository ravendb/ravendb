using System;

namespace Raven.Server.Config;

public class PersistConfigurationException : Exception
{
    public PersistConfigurationException(string message, Exception exception) 
        : base(message, exception)
    {
        
    }
}
