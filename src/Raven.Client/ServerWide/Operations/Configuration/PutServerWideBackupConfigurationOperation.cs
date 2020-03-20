using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class PutServerWideBackupConfigurationOperation : IServerOperation<PutServerWideBackupConfigurationResponse>
    {
        private readonly ServerWideBackupConfiguration _configuration;

        public PutServerWideBackupConfigurationOperation(ServerWideBackupConfiguration configuration)
        {
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        }

        public RavenCommand<PutServerWideBackupConfigurationResponse> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutServerWideBackupConfigurationCommand(context, _configuration);
        }

        private class PutServerWideBackupConfigurationCommand : RavenCommand<PutServerWideBackupConfigurationResponse>, IRaftCommand
        {
            private readonly BlittableJsonReaderObject _configuration;

            public PutServerWideBackupConfigurationCommand(JsonOperationContext context, ServerWideBackupConfiguration configuration)
            {
                if (configuration == null)
                    throw new ArgumentNullException(nameof(configuration));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                _configuration = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration, context);
            }

            public override bool IsReadRequest => false;

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/backup";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, _configuration);
                    })
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.PutServerWideBackupConfigurationResponse(response);
            }
        }
    }

    public class PutServerWideConfigurationResponse : IDynamicJson
    {
        public string Name { get; set; }

        public long RaftCommandIndex { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(RaftCommandIndex)] = RaftCommandIndex
            };
        }
    }

    public class PutServerWideBackupConfigurationResponse : PutServerWideConfigurationResponse
    {

    }
}
