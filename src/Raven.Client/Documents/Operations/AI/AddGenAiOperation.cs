using System.Net.Http;
using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class AddGenAiOperation(GenAiConfiguration configuration, StartingPointChangeVector startingPointChangeVector = null) : IMaintenanceOperation<AddEtlOperationResult>
{
    public RavenCommand<AddEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new AddGenAiCommand(conventions, configuration, startingPointChangeVector);
    }

    internal sealed class AddGenAiCommand : AddEtlOperation<AiConnectionString>.AddEtlCommand
    {
        private readonly StartingPointChangeVector _startingPointChangeVector;

        public AddGenAiCommand(DocumentConventions conventions, GenAiConfiguration configuration, StartingPointChangeVector startingPointChangeVector = null):base(conventions, configuration)
        {
            _startingPointChangeVector = startingPointChangeVector ?? StartingPointChangeVector.LastDocument;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);

            url += $"?changeVector={Uri.EscapeDataString(_startingPointChangeVector.Value)}";

            return request;
        }
    }
}
