using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetDocumentCommand : RavenCommand<GetDocumentResult>
    {
        private readonly string _id;

        private readonly string[] _ids;
        private readonly string[] _includes;

        private readonly bool _metadataOnly;

        private readonly string _startWith;
        private readonly string _matches;
        private readonly int _start;
        private readonly int _pageSize;
        private readonly string _exclude;
        private readonly string _startAfter;

        public GetDocumentCommand(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public GetDocumentCommand(string id, string[] includes, bool metadataOnly)
        {
            _id = id;
            _includes = includes;
            _metadataOnly = metadataOnly;
        }

        public GetDocumentCommand(string[] ids, string[] includes, string transformer, Dictionary<string, object> transformerParameters, bool metadataOnly)
        {
            if (ids == null || ids.Length == 0)
                throw new ArgumentNullException(nameof(ids));

            _ids = ids;
            _includes = includes;
            _metadataOnly = metadataOnly;
        }

        public GetDocumentCommand(string startWith, string startAfter, string matches, string exclude, int start, int pageSize)
        {
            _startWith = startWith ?? throw new ArgumentNullException(nameof(startWith));
            _startAfter = startAfter;
            _matches = matches;
            _exclude = exclude;
            _start = start;
            _pageSize = pageSize;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append("/databases/")
                .Append(node.Database)
                .Append("/docs?");

            if (_metadataOnly)
                pathBuilder.Append("&metadata-only=true");

            if (_startWith != null)
            {
                pathBuilder.Append($"startsWith={Uri.EscapeDataString(_startWith)}&start={_start.ToInvariantString()}&pageSize={_pageSize.ToInvariantString()}");

                if (_matches != null)
                    pathBuilder.Append($"&matches={_matches}");
                if (_exclude != null)
                    pathBuilder.Append($"&exclude={_exclude}");
                if (_startAfter != null)
                    pathBuilder.Append($"&startAfter={Uri.EscapeDataString(_startAfter)}");
            }

            if (_includes != null)
            {
                foreach (var include in _includes)
                {
                    pathBuilder.Append($"&include={include}");
                }
            }

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            if (_id != null)
            {
                pathBuilder.Append($"&id={Uri.EscapeDataString(_id)}");
            }
            else if (_ids != null)
            {
                PrepareRequestWithMultipleIds(pathBuilder, request, _ids, ctx);
            }

            url = pathBuilder.ToString();
            return request;
        }

        public static void PrepareRequestWithMultipleIds(StringBuilder pathBuilder, HttpRequestMessage request, string[] ids, JsonOperationContext context)
        {
            var uniqueIds = new HashSet<string>(ids);
            // if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
            // we are fine with that, requests to load > 1024 items are going to be rare
            var isGet = uniqueIds.Sum(x => x.Length) < 1024;
            if (isGet)
            {
                uniqueIds.ApplyIfNotNull(id => pathBuilder.Append($"&id={Uri.EscapeDataString(id)}"));
            }
            else
            {
                request.Method = HttpMethod.Post;

                request.Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(context, stream))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray("Ids", uniqueIds);
                        writer.WriteEndObject();
                    }
                });
            }
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.GetDocumentResult(response);
        }

        public override bool IsReadRequest => true;
    }
}
