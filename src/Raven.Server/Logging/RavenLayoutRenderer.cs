using System;
using System.Text;
using NLog;
using NLog.Config;
using NLog.LayoutRenderers;
using Raven.Server.Rachis;

namespace Raven.Server.Logging;

[LayoutRenderer("rvn")]
[ThreadAgnostic]
internal sealed class RavenLayoutRenderer : LayoutRenderer
{
    public static string NodeTag;

    [DefaultParameter]
    public string Item { get; set; }

    protected override void Append(StringBuilder builder, LogEventInfo logEvent)
    {
        switch (Item)
        {
            case "NodeTag":
                builder.Append(NodeTag ?? RachisConsensus.InitialTag);
                break;
            default:
                throw new ArgumentException($"Unknown RavenDB layout renderer item: '{Item}'");
        }
    }
}
