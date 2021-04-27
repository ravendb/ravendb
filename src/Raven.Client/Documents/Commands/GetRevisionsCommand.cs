using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetRevisionsCommand : RavenCommand<BlittableArrayResult>
    {
        private readonly string _changeVector;
        public readonly string[] ChangeVectors;
        private readonly string _id;
        private readonly int? _start;
        private readonly int? _pageSize;
        private readonly bool _metadataOnly;
        private readonly DateTime? _before;

        public GetRevisionsCommand(string id, DateTime before)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _before = before;
        }

        public GetRevisionsCommand(string id, int? start, int? pageSize, bool metadataOnly = false)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _start = start;
            _pageSize = pageSize;
            _metadataOnly = metadataOnly;
        }

        public GetRevisionsCommand(string changeVector, bool metadataOnly = false)
        {
            _changeVector = changeVector ?? throw new ArgumentNullException(nameof(changeVector));
            _metadataOnly = metadataOnly;
        }

        public GetRevisionsCommand(string[] changeVectors, bool metadataOnly = false)
        {
            ChangeVectors = changeVectors ?? throw new ArgumentNullException(nameof(changeVectors));
            _metadataOnly = metadataOnly;
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
                .Append("/revisions?");
            GetRequestQueryString(pathBuilder);

            url = pathBuilder.ToString();
            return request;
        }

        public string GetRequestQueryString()
        {
            var sb = new StringBuilder("?");
            GetRequestQueryString(sb);
            return sb.ToString();
        }
        
        private void GetRequestQueryString(StringBuilder pathBuilder)
        {

            if (_id != null)
                pathBuilder.Append("&id=").Append(Uri.EscapeDataString(_id));
            else if (_changeVector != null)
                pathBuilder.Append("&changeVector=").Append(Uri.EscapeDataString(_changeVector));
            else if (ChangeVectors != null)
            {
                foreach (var changeVector in ChangeVectors)
                {
                    pathBuilder.Append("&changeVector=").Append(Uri.EscapeDataString(changeVector));
                }
            }

            if (_before.HasValue)
                pathBuilder.Append("&before=").Append(_before.Value.GetDefaultRavenFormat());
            if (_start.HasValue)
                pathBuilder.Append("&start=").Append(_start);
            if (_pageSize.HasValue)
                pathBuilder.Append("&pageSize=").Append(_pageSize);
            if (_metadataOnly)
                pathBuilder.Append("&metadataOnly=true");
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }
            if (fromCache)
            {
                // we have to clone the response here because  otherwise the cached item might be freed while
                // we are still looking at this result, so we clone it to the side
                response = response.Clone(context);
            }
            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }

        public override bool IsReadRequest => true;
    }
}
