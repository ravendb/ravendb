using System;

namespace Raven.Client.Exceptions.Commercial;

public class PortInUseException : RavenException
{
    public int Port { get; }

    public PortInUseException()
    {
    }

    public PortInUseException(string message)
        : base(message)
    {
    }

    public PortInUseException(int port, string message)
        : base(message)
    {
        Port = port;
    }

    public PortInUseException(string message, Exception e)
        : base(message, e)
    {
    }
}
