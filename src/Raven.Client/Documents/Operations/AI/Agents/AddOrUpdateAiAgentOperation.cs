using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents
{
    public class AddOrUpdateAiAgentOperation : IMaintenanceOperation<AiAgentConfigurationResult>
    {
        private readonly AiAgentConfiguration _configuration;

        public AddOrUpdateAiAgentOperation(AiAgentConfiguration configuration)
        {
            ValidationMethods.AssertNotNullOrEmpty(configuration, nameof(configuration));
            
            if (HasNoSampleObjectOrSchema(configuration))
                throw new ArgumentException($"Please provide a non-empty value for either {configuration.OutputSchema} or {nameof(configuration.SampleObject)} is required.");

            _configuration = configuration;
        }

        private static bool HasNoSampleObjectOrSchema(AiAgentConfiguration configuration) => 
            string.IsNullOrWhiteSpace(configuration.OutputSchema) && string.IsNullOrWhiteSpace(configuration.SampleObject);

        public static AddOrUpdateAiAgentOperation Create<T>(AiAgentConfiguration configuration, T outputType)
        {
            if (HasNoSampleObjectOrSchema(configuration))
            {
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    configuration.SampleObject = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(outputType, ctx).ToString();
                }
            }

            return new AddOrUpdateAiAgentOperation(configuration);
        }

        public RavenCommand<AiAgentConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new AddOrUpdateAiAgentOperationCommand(_configuration, conventions);
        }

        private sealed class AddOrUpdateAiAgentOperationCommand : RavenCommand<AiAgentConfigurationResult>, IRaftCommand
        {
            private readonly AiAgentConfiguration _configuration;
            private readonly DocumentConventions _conventions;

            public AddOrUpdateAiAgentOperationCommand(AiAgentConfiguration configuration, DocumentConventions conventions)
            {
                _configuration = configuration;
                _conventions = conventions;
            }
            public override bool IsReadRequest => false;
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/ai/agent";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx)).ConfigureAwait(false);
                    }, _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.AiAgentConfigurationResult(response);
            }

            public string RaftUniqueRequestId => RaftIdGenerator.NewId();
        }
    }
}
