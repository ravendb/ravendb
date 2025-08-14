using System;
using Raven.Client.Exceptions;

namespace Raven.Server.Exceptions;

public class SmugglerException : RavenException
{
    public SmugglerException(string message, Exception exception) 
        : base(message, exception)
    {
        
    }
}
