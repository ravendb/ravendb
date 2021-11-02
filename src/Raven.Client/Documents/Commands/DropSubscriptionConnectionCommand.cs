using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class DropSubscriptionConnectionCommand : RavenCommand
    {
        private readonly string _name;
        private readonly string _workerId;

        public DropSubscriptionConnectionCommand(string name)
        {
            _name = name;
        }
        public DropSubscriptionConnectionCommand(string name, string workerId)
        {
            _name = name;
            _workerId = workerId;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var path = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/subscriptions/drop");
            
            if(string.IsNullOrEmpty(_name) == false)
            {
                path.Append("?name=").Append(Uri.EscapeDataString(_name));

                if (string.IsNullOrEmpty(_workerId) == false)
                {
                    path.Append("&workerId=").Append(_workerId);
                }
            }
            
            url = path.ToString();
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
            return request;
        }
    }
}
