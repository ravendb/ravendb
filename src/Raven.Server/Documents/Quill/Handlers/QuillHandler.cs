using System.Threading.Tasks;
using Raven.Server.Documents.Quill.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.Documents.Quill.Handlers;

public sealed class QuillHandler : ServerRequestHandler
{
    [RavenAction("/quill/ai/assist", "POST", AuthorizationStatus.Operator)]
    public async Task AiAssist()
    {
        using (var processor = new QuillAiAssistProcessor(this))
            await processor.ExecuteAsync();
    }
}
