using System;
using System.Text;
using Sparrow.Json;

namespace Raven.Client.Exceptions.Commercial;

public class PortInUseException : RavenException
{

    public PortInUseException()
    {
    }

    public PortInUseException(string message)
        : base(message)
    {
    }

    public PortInUseException(int port, string address, string message, Exception e = null)
        : base(BuildMessage(message, port, address),e)
    {
    }

    private static string BuildMessage(string message, int port, string address)
    {
        var result = new StringBuilder(message?.Length ?? 0);

        result.Append(" Port:")
            .Append(port)
            .Append(" Address:")
            .Append(address)
            .Append(message);

        return result.ToString();
    }
}
