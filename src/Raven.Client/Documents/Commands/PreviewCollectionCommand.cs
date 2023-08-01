using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    internal sealed class PreviewCollectionCommand : RavenCommand<BlittableJsonReaderObject>
    {
        private readonly string _collection;

        public string Collection => _collection;

        public PreviewCollectionCommand(string collection)
        {
            _collection = collection;
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
                .Append("/studio/collections/preview");

            if (string.IsNullOrEmpty(_collection) == false)
            {
                pathBuilder.Append($"?collection={Uri.EscapeDataString(_collection)}");
            }

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
