using System;
using System.Diagnostics.CodeAnalysis;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.AiAgent
{
    internal class AddOrModifyAiAgentOperation : AddOrModifyAiAgentOperation<object>
    {
        public AddOrModifyAiAgentOperation(string name, AiAgentConfiguration configuration) : base(name, configuration)
        {
            if (string.IsNullOrEmpty(configuration.OutputSchema))
                throw new ArgumentException("OutputSchema cannot be null or empty.", nameof(configuration.OutputSchema));
        }
    }

    internal class AddOrModifyAiAgentOperation<TSchema> : IMaintenanceOperation<AiAgentConfigurationResult> where TSchema : new()
    {
        private readonly string _name;
        private readonly AiAgentConfiguration _configuration;

        public AddOrModifyAiAgentOperation(string name, AiAgentConfiguration configuration)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _configuration = configuration ?? throw new ArgumentNullException(name);
            
        }

        protected void Initialize()
        {
            if (_configuration.OutputSchema == null)
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    _configuration.OutputSchema = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new TSchema(), context).ToString();
                }
            }
        }

        public RavenCommand<AiAgentConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            Initialize();
            return new AddOrModifyAiAgentOperationCommand(_name, _configuration, conventions);
        }

        private sealed class AddOrModifyAiAgentOperationCommand : RavenCommand<AiAgentConfigurationResult>, IRaftCommand
        {
            private readonly string _name;
            private readonly AiAgentConfiguration _configuration;
            private readonly DocumentConventions _conventions;

            public AddOrModifyAiAgentOperationCommand(string name, AiAgentConfiguration configuration, DocumentConventions conventions)
            {
                _name = name;
                _configuration = configuration;
                _conventions = conventions;
            }
            public override bool IsReadRequest => false;
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/ai/ai-agent/add?agent={_name}";

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
