using System.Net.Http;
using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI;

public class UpdateGenAiOperation(long taskId, GenAiConfiguration configuration, string initialChangeVector = null) : IMaintenanceOperation<UpdateEtlOperationResult>
{
    public RavenCommand<UpdateEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new UpdateGenAiCommand(conventions, taskId, configuration, initialChangeVector);
    }

    internal sealed class UpdateGenAiCommand : UpdateEtlOperation<AiConnectionString>.UpdateEtlCommand
    {
        private readonly string _initialChangeVector;

        public UpdateGenAiCommand(DocumentConventions conventions, long taskId, GenAiConfiguration configuration, string initialChangeVector): base(conventions, taskId, configuration)
        {
            _initialChangeVector = initialChangeVector ?? nameof(Constants.Documents.GenAiChangeVectorSpecialStates.DoNotChange);
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);

            url += $"&changeVector={Uri.EscapeDataString(_initialChangeVector)}";

            return request;
        }
    }
}
