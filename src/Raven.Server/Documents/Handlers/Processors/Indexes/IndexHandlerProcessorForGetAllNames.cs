using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal sealed class IndexHandlerProcessorForGetAllNames : AbstractIndexHandlerProcessorForGetAllNames<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForGetAllNames([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<string[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    protected override string[] GetIndexNames(string name)
    {
        return IndexHandlerProcessorForGetAll.GetIndexDefinitions(RequestHandler, name, 0, int.MaxValue)
            .Select(x => x.Name)
            .ToArray();
    }
}
