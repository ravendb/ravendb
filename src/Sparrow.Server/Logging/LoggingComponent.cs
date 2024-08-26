using System;

namespace Sparrow.Server.Logging;

public sealed class LoggingComponent
{
    private readonly string _name;

    private LoggingComponent(string name)
    {
        _name = name;
    }

    public override string ToString()
    {
        return _name;
    }

    public static readonly LoggingComponent Tcp = new("TCP");

    public static readonly LoggingComponent Configuration = new("Configuration");

    public static readonly LoggingComponent ServerStore = new("ServerStore");

    public static LoggingComponent RemoteConnection(string src, string dest)
    {
        if (src == null)
            throw new ArgumentNullException(nameof(src));
        if (dest == null)
            throw new ArgumentNullException(nameof(dest));

        return new($"{src} > {dest}");
    }

    public static LoggingComponent Name(string name)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        return new(name);
    }

    public static LoggingComponent NodeTag(string nodeTag)
    {
        if (nodeTag == null)
            throw new ArgumentNullException(nameof(nodeTag));

        return new(nodeTag);
    }
}
