using System.Net.Http;
using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class AddGenAiOperation(GenAiConfiguration configuration, StartingPointChangeVector startingPoint = null) : IMaintenanceOperation<AddEtlOperationResult>
{
    public RavenCommand<AddEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new AddGenAiCommand(conventions, configuration, startingPoint);
    }

    internal sealed class AddGenAiCommand : AddEtlOperation<AiConnectionString>.AddEtlCommand
    {
        private readonly StartingPointChangeVector _startingPoint;

        public AddGenAiCommand(DocumentConventions conventions, GenAiConfiguration configuration, StartingPointChangeVector startingPoint = null):base(conventions, configuration)
        {
            _startingPoint = startingPoint ?? StartingPointChangeVector.LastDocument;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);

            url += $"?changeVector={Uri.EscapeDataString(_startingPoint.Value)}";

            return request;
        }
    }
}
