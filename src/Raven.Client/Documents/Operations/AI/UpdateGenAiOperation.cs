using System.Net.Http;
using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class UpdateGenAiOperation(long taskId, GenAiConfiguration configuration, StartingPointChangeVector startingPointChangeVector = null) : IMaintenanceOperation<UpdateEtlOperationResult>
{
    public RavenCommand<UpdateEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new UpdateGenAiCommand(conventions, taskId, configuration, startingPointChangeVector);
    }

    internal sealed class UpdateGenAiCommand : UpdateEtlOperation<AiConnectionString>.UpdateEtlCommand
    {
        private readonly StartingPointChangeVector _startingPointChangeVector;

        public UpdateGenAiCommand(DocumentConventions conventions, long taskId, GenAiConfiguration configuration, StartingPointChangeVector startingPointChangeVector): base(conventions, taskId, configuration)
        {
            _startingPointChangeVector = startingPointChangeVector ?? StartingPointChangeVector.DoNotChange;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);

            url += $"&changeVector={Uri.EscapeDataString(_startingPointChangeVector.Value)}";

            return request;
        }
    }
}
