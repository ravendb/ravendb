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
    public class AddOrUpdateAiAgentOperation : AddOrUpdateAiAgentOperation<object>
    {
        public AddOrUpdateAiAgentOperation(string name, AiAgentConfiguration configuration) : base(name, configuration)
        {
            if (string.IsNullOrWhiteSpace(configuration.OutputSchema) && string.IsNullOrWhiteSpace(configuration.SampleObject))
                throw new ArgumentException($"Please provide a non-empty value for either {configuration.OutputSchema} or {nameof(configuration.SampleObject)} is required.");
        }
    }

    public class AddOrUpdateAiAgentOperation<TSchema> : IMaintenanceOperation<AiAgentConfigurationResult> where TSchema : new()
    {
        private readonly string _name;
        private readonly AiAgentConfiguration _configuration;
        private static readonly TSchema Instance = new();

        public AddOrUpdateAiAgentOperation(string name, AiAgentConfiguration configuration)
        {
            ValidationMethods.AssertNotNullOrEmpty(configuration, nameof(configuration));
            ValidationMethods.AssertNotNullOrEmpty(name, nameof(name));

            _name = name;
            _configuration = configuration;
        }

        public RavenCommand<AiAgentConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new AddOrUpdateAiAgentOperationCommand(_name, _configuration, conventions);
        }

        private sealed class AddOrUpdateAiAgentOperationCommand : RavenCommand<AiAgentConfigurationResult>, IRaftCommand
        {
            private readonly string _name;
            private readonly AiAgentConfiguration _configuration;
            private readonly DocumentConventions _conventions;

            public AddOrUpdateAiAgentOperationCommand(string name, AiAgentConfiguration configuration, DocumentConventions conventions)
            {
                _name = name;
                _configuration = configuration;
                _conventions = conventions;
            }
            public override bool IsReadRequest => false;
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/ai/agent?name={Uri.EscapeDataString(_name)}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        _configuration.SampleObject ??= DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(Instance, ctx).ToString();
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
