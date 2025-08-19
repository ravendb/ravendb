using System;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractDatabaseTasksHandlerProcessorForRestartDatabase<TRequestHandler, TOperationContext> : AbstractHandlerProxyNoContentProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    private readonly string _urlSuffix;

    protected AbstractDatabaseTasksHandlerProcessorForRestartDatabase([NotNull] TRequestHandler requestHandler, [NotNull] string urlSuffix)
        : base(requestHandler)
    {
        if (string.IsNullOrEmpty(urlSuffix)) 
            throw new ArgumentException("Value cannot be null or empty.", nameof(urlSuffix));
        
        _urlSuffix = urlSuffix;
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        await ServerStore.DatabasesLandlord.RestartDatabaseAsync(RequestHandler.DatabaseName);
    }

    protected override RavenCommand<object> CreateCommandForNode(string nodeTag) => new RestartDatabaseCommand(nodeTag, _urlSuffix);

    private sealed class RestartDatabaseCommand : RavenCommand
    {
        private readonly string _urlSuffix;

        public RestartDatabaseCommand(string nodeTag, string urlSuffix)
        {
            _urlSuffix = urlSuffix;
            SelectedNodeTag = nodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}{_urlSuffix}";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
        }
    }
}
