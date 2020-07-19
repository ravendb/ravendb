using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Commands
{
    public class GetTcpInfoCommand : RavenCommand<TcpConnectionInfo>
    {
        private readonly string _tag;
        private readonly string _dbName;
        private readonly string _dbId;
        private readonly long _etag;
        private readonly bool _fromReplication;

        public GetTcpInfoCommand(string tag)
        {
            _tag = tag;
            Timeout = TimeSpan.FromSeconds(15);
        }

        public GetTcpInfoCommand(string tag, string dbName = null) : this(tag)
        {
            _dbName = dbName;
        }

        internal GetTcpInfoCommand(string tag, string dbName, string dbId, long etag) : this(tag, dbName)
        {
            _dbId = dbId;
            _etag = etag;
            _fromReplication = true;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            if (string.IsNullOrEmpty(_dbName))
            {
                url = $"{node.Url}/info/tcp?tag={_tag}";
            }
            else
            {
                url = $"{node.Url}/databases/{_dbName}/info/tcp?tag={_tag}";
                if (_fromReplication)
                {
                    url += $"&from-outgoing={_dbId}&etag={_etag}";
                }
            }
            RequestedNode = node;
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.TcpConnectionInfo(response);
        }

        public ServerNode RequestedNode { get; private set; }

        public override bool IsReadRequest => true;   
    }
}
