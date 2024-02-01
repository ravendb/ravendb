using System;
using Raven.Client.Exceptions;

namespace Raven.Server.Exceptions;

public class PersistConfigurationException : RavenException
{
    public PersistConfigurationException(string message, Exception exception) 
        : base(message, exception)
    {
        
    }
}
