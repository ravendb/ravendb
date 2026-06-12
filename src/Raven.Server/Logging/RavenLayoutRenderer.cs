using System.Runtime.CompilerServices;
using System.Text;
using NLog;
using NLog.Config;
using NLog.LayoutRenderers;
using Raven.Server.Rachis;

namespace Raven.Server.Logging;

[LayoutRenderer("rvn")]
[ThreadAgnostic]
internal sealed class RvnLayoutRenderer : LayoutRenderer
{
    internal static volatile string NodeTag;

    [DefaultParameter]
    public string Item { get; set; }

    public string Default { get; set; }

    protected override void Append(StringBuilder builder, LogEventInfo logEvent)
    {
        builder.Append(Resolve(Item) ?? Default ?? string.Empty);
    }

    private static string Resolve(string item) => item switch
    {
        "NodeTag" => NodeTag ?? RachisConsensus.InitialTag,
        _ => null
    };

    [ModuleInitializer]
    internal static void Register() =>
        LogManager.Setup().SetupExtensions(ext => ext.RegisterLayoutRenderer<RvnLayoutRenderer>("rvn"));
}
