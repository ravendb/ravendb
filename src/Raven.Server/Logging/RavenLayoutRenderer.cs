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
}

internal static class RvnLayoutRendererExtensions
{
    public static ISetupBuilder RegisterRavenLayoutRenderers(this ISetupBuilder setupBuilder) =>
        setupBuilder.SetupExtensions(ext => ext.RegisterLayoutRenderer<RvnLayoutRenderer>("rvn"));

    // Must run at module load, before any layout containing ${rvn:...} is parsed. NLog parses layout
    // strings eagerly when targets are constructed - including static field initializers in
    // RavenLogManagerServerExtensions (ConsoleRule, PipeRule, AdminLogsRule -> AdminLogsTarget). Keeping this
    // in a type with no layout-parsing static state ensures registration completes before any of those run.
    [ModuleInitializer]
    internal static void Register() => LogManager.Setup().RegisterRavenLayoutRenderers();
}
