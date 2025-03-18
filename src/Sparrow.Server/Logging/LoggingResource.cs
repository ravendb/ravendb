using System;

namespace Sparrow.Server.Logging;

public sealed class LoggingResource
{
    private readonly string _name;

    private LoggingResource(string name)
    {
        _name = name;
    }

    public override string ToString()
    {
        return _name;
    }

    public static readonly LoggingResource Server = new("Server");

    public static readonly LoggingResource Cluster = new("Cluster");

    public static readonly LoggingResource Voron = new("Voron");

    public static LoggingResource Database(string databaseName)
    {
        if (databaseName == null) 
            throw new ArgumentNullException(nameof(databaseName));

        return new LoggingResource(databaseName);
    }
}
