﻿using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Commands
{
    public class GetTcpInfoCommand : RavenCommand<TcpConnectionInfo>
    {
        private readonly string _tag;
        private string _dbName;

        public GetTcpInfoCommand(string tag)
        {
            _tag = tag;
        }

        public GetTcpInfoCommand(string tag, string dbName = null) : this(tag)
        {
            _dbName = dbName;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            if (string.IsNullOrEmpty(_dbName))
            {
                url = $"{node.Url}/info/tcp?tag={_tag}";

            }
            else
            {
                url = $"{node.Url}/databases/{_dbName}/info/tcp?tag={_tag}";
                
            }
            RequestedNode = node;
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.TcpConnectionInfo(response);
        }

        public ServerNode RequestedNode { get; private set; }

        public override bool IsReadRequest => true;   
    }
}