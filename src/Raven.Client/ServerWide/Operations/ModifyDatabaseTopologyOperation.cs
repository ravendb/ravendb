using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class ModifyDatabaseTopologyOperation : IServerOperation<ModifyDatabaseTopologyResult>
    {
        private readonly string _databaseName;
        private readonly DatabaseTopology _databaseTopology;

        public ModifyDatabaseTopologyOperation(string databaseName, DatabaseTopology databaseTopology)
        {
            if (databaseTopology == null)
                throw new ArgumentNullException(nameof(databaseTopology));

            _databaseTopology = databaseTopology;

            ResourceNameValidator.AssertValidDatabaseName(databaseName);
            _databaseName = databaseName;
        }

        public RavenCommand<ModifyDatabaseTopologyResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new ModifyDatabaseTopologyCommand(_databaseName, _databaseTopology);
        }

        internal class ModifyDatabaseTopologyCommand : RavenCommand<ModifyDatabaseTopologyResult>, IRaftCommand
        {
            private readonly DatabaseTopology _databaseTopology;
            private readonly string _databaseName;

            public ModifyDatabaseTopologyCommand(string databaseName, DatabaseTopology databaseTopology)
            {
                _databaseTopology = databaseTopology;
                _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseTopology));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/topology/modify?name={_databaseName}";

                var topologyDocument = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_databaseTopology, ctx);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, topologyDocument).ConfigureAwait(false))
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifyDatabaseTopologyResult(response);
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
