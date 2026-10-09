using JetBrains.Annotations;
using Raven.Server.Web;

namespace Raven.Server.Documents.AI.AiAssistant.Handlers.Processors;

internal sealed class AiAssistantMigrationProcessor([NotNull] RequestHandler requestHandler, string upstreamPath) : AiAssistantAssistProcessor(requestHandler)
{
    protected override string UpstreamPath => upstreamPath;
}
