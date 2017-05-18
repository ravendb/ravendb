using System;
using System.Net.Http;
using Raven.Client.Http;

namespace Raven.Client.Server.Commands
{
    public class CloseTcpConnectionCommand : RavenCommand
    {
        private readonly long _id;

        public CloseTcpConnectionCommand(long id)
        {
            _id = id;
        }

        

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/tcp?id={_id}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
        }
        }
}