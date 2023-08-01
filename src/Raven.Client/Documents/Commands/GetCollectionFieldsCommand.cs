using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    internal sealed class GetCollectionFieldsCommand : RavenCommand<BlittableJsonReaderObject>
    {
        private readonly string _collection;
        private readonly string _prefix;

        public string Collection => _collection;
        public string Prefix => _prefix;

        public GetCollectionFieldsCommand(string collection, string prefix)
        {
            _collection = collection;
            _prefix = prefix;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            var pathBuilder = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/studio/collections/fields");

            if (string.IsNullOrEmpty(_collection) == false && string.IsNullOrEmpty(_prefix) == false)
            {
                pathBuilder.Append($"?collection={Uri.EscapeDataString(_collection)}");
                pathBuilder.Append($"&prefix={Uri.EscapeDataString(_prefix)}");
            }
            else if (string.IsNullOrEmpty(_collection) == false)
                pathBuilder.Append($"?collection={Uri.EscapeDataString(_collection)}");
            else if (string.IsNullOrEmpty(_prefix) == false)
                pathBuilder.Append($"?prefix={Uri.EscapeDataString(_prefix)}");

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = response.Clone(context);
        }

        public override bool IsReadRequest => true;
    }
}
