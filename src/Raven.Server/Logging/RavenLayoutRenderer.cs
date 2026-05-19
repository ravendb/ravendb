using System.Text;
using NLog;
using NLog.Config;
using NLog.LayoutRenderers;
using Raven.Server.Rachis;

namespace Raven.Server.Logging;

[LayoutRenderer("nodeTag")]
[ThreadAgnostic]
internal sealed class NodeTagLayoutRenderer : LayoutRenderer
{
    public static volatile string NodeTag;

    protected override void Append(StringBuilder builder, LogEventInfo logEvent)
    {
        builder.Append(NodeTag ?? RachisConsensus.InitialTag);
    }
}
