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
        private readonly long? _subscriptionTaskId;

        public DropSubscriptionConnectionCommand(string name)
        {
            _name = name;
        }

        internal DropSubscriptionConnectionCommand(string name, string workerId) : this(name)
        {
            if (string.IsNullOrEmpty(workerId))
                throw new ArgumentException($"{nameof(workerId)} can't be null or empty");

            _workerId = workerId;
        }

        internal DropSubscriptionConnectionCommand(string name, long? subscriptionTaskId, string workerId) : this(name, workerId)
        {
            _subscriptionTaskId = subscriptionTaskId;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var path = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/subscriptions/drop");

            if (string.IsNullOrEmpty(_name) == false && _subscriptionTaskId.HasValue)
            {
                path.Append("?name=").Append(Uri.EscapeDataString(_name));
                path.Append("&id=").Append(_subscriptionTaskId);
            }
            else if (string.IsNullOrEmpty(_name) == false)
            {
                path.Append("?name=").Append(Uri.EscapeDataString(_name));
            }
            else
            {
                path.Append("?id=").Append(_subscriptionTaskId);
            }

            if (string.IsNullOrEmpty(_workerId) == false)
            {
                path.Append("&workerId=").Append(Uri.EscapeDataString(_workerId));
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
