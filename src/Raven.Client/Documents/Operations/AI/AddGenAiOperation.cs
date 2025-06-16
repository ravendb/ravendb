using System.Net.Http;
using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class AddGenAiOperation(GenAiConfiguration configuration, string initialChangeVector = null) : IMaintenanceOperation<AddEtlOperationResult>
{
    public RavenCommand<AddEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new AddGenAiCommand(conventions, configuration, initialChangeVector);
    }

    internal sealed class AddGenAiCommand : AddEtlOperation<AiConnectionString>.AddEtlCommand
    {
        private readonly string _initialChangeVector;

        public AddGenAiCommand(DocumentConventions conventions, GenAiConfiguration configuration, string initialChangeVector):base(conventions, configuration)
        {
            _initialChangeVector = initialChangeVector ?? nameof(Constants.Documents.GenAiChangeVectorSpecialStates.LastDocument);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);

            url += $"?changeVector={Uri.EscapeDataString(_initialChangeVector)}";

            return request;
        }
    }
}
