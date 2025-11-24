using System.Net.Http;
using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class UpdateGenAiOperation(long taskId, GenAiConfiguration configuration, StartingPointChangeVector startingPoint = null, List<string> transformationsToReset = null) : IMaintenanceOperation<UpdateEtlOperationResult>
{
    public RavenCommand<UpdateEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new UpdateGenAiCommand(conventions, taskId, configuration, startingPoint, transformationsToReset);
    }

    internal sealed class UpdateGenAiCommand : UpdateEtlOperation<AiConnectionString>.UpdateEtlCommand
    {
        private readonly StartingPointChangeVector _startingPoint;

        public UpdateGenAiCommand(DocumentConventions conventions, long taskId, GenAiConfiguration configuration, StartingPointChangeVector startingPoint, List<string> transformationsToReset): base(conventions, taskId, configuration, transformationsToReset)
        {
            _startingPoint = startingPoint ?? StartingPointChangeVector.DoNotChange;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);

            url += $"&changeVector={Uri.EscapeDataString(_startingPoint.Value)}";

            return request;
        }
    }
}
