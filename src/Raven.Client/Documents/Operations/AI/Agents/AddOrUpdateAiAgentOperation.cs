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
    /// <summary>
    /// Creates or updates an AI agent configuration on the server.
    /// </summary>
    public class AddOrUpdateAiAgentOperation : IMaintenanceOperation<AiAgentConfigurationResult>
    {
        private readonly object _sampleObject;
        private readonly AiAgentConfiguration _configuration;

        /// <summary>
        /// Initializes a new instance of <see cref="AddOrUpdateAiAgentOperation"/> with the specified <paramref name="configuration"/>.
        /// </summary>
        /// <param name="configuration">The agent configuration to store.</param>
        public AddOrUpdateAiAgentOperation(AiAgentConfiguration configuration)
        {
            ValidationMethods.AssertNotNullOrEmpty(configuration, nameof(configuration));
            _configuration = configuration;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="AddOrUpdateAiAgentOperation"/> with the specified <paramref name="configuration"/> and <paramref name="sampleObject"/>.
        /// </summary>
        /// <param name="configuration">The agent configuration to store.</param>
        /// <param name="sampleObject">A sample object used by the server to infer the output schema.</param>
        public AddOrUpdateAiAgentOperation(AiAgentConfiguration configuration, object sampleObject) : this(configuration)
        {
            _sampleObject = sampleObject;
        }

        private static bool HasNoSampleObjectOrSchema(AiAgentConfiguration configuration) => 
            string.IsNullOrWhiteSpace(configuration.OutputSchema) && string.IsNullOrWhiteSpace(configuration.SampleObject);

        /// <summary>
        /// Creates a new instance of <see cref="AddOrUpdateAiAgentOperation"/> with the specified <paramref name="configuration"/> and <paramref name="outputType"/>.
        /// </summary>
        /// <typeparam name="T">The schema sample type.</typeparam>
        /// <param name="configuration">The agent configuration to store.</param>
        /// <param name="outputType">A sample object used by the server to infer the output schema.</param>
        public static AddOrUpdateAiAgentOperation Create<T>(AiAgentConfiguration configuration, T outputType) => new(configuration, outputType);

        /// <summary>
        /// Creates the command to send to the server.
        /// </summary>
        /// <param name="conventions">Document conventions used for serialization.</param>
        /// <param name="context">JSON operation context.</param>
        /// <returns>The command instance.</returns>
        public RavenCommand<AiAgentConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            if (_sampleObject != null && HasNoSampleObjectOrSchema(_configuration))
            {
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    _configuration.SampleObject = conventions.Serialization.DefaultConverter.ToBlittable(_sampleObject, ctx).ToString();
                }
            }

            return new AddOrUpdateAiAgentOperationCommand(_configuration, conventions);
        }

        private sealed class AddOrUpdateAiAgentOperationCommand : RavenCommand<AiAgentConfigurationResult>, IRaftCommand
        {
            private readonly AiAgentConfiguration _configuration;
            private readonly DocumentConventions _conventions;

            public AddOrUpdateAiAgentOperationCommand(AiAgentConfiguration configuration, DocumentConventions conventions)
            {
                if (HasNoSampleObjectOrSchema(configuration))
                    throw new ArgumentException($"Please provide a non-empty value for either {configuration.OutputSchema} or {nameof(configuration.SampleObject)} is required.");

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
