using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetDocumentsCommand : RavenCommand<GetDocumentsResult>
    {
        private readonly string _id;

        private readonly string[] _ids;
        private readonly string[] _includes;

        private readonly bool _metadataOnly;

        private readonly string _startWith;
        private readonly string _matches;
        private readonly int? _start;
        private readonly int? _pageSize;
        private readonly string _exclude;
        private readonly string _startAfter;

        public GetDocumentsCommand(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public GetDocumentsCommand(string id, string[] includes, bool metadataOnly)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _includes = includes;
            _metadataOnly = metadataOnly;
        }

        public GetDocumentsCommand(string[] ids, string[] includes, bool metadataOnly)
        {
            if (ids == null || ids.Length == 0)
                throw new ArgumentNullException(nameof(ids));

            _ids = ids;
            _includes = includes;
            _metadataOnly = metadataOnly;
        }

        public GetDocumentsCommand(string startWith, string startAfter, string matches, string exclude, int start, int pageSize, bool metadataOnly)
        {
            _startWith = startWith ?? throw new ArgumentNullException(nameof(startWith));
            _startAfter = startAfter;
            _matches = matches;
            _exclude = exclude;
            _start = start;
            _pageSize = pageSize;
            _metadataOnly = metadataOnly;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append("/databases/")
                .Append(node.Database)
                .Append("/docs?");

            if (_start.HasValue)
                pathBuilder.Append("&start=").Append(_start);
            if (_pageSize.HasValue)
                pathBuilder.Append("&pageSize=").Append(_pageSize);
            if (_metadataOnly)
                pathBuilder.Append("&metadataOnly=true");

            if (_startWith != null)
            {
                pathBuilder.Append("&startsWith=").Append(Uri.EscapeDataString(_startWith));

                if (_matches != null)
                    pathBuilder.Append("&matches=").Append(_matches);
                if (_exclude != null)
                    pathBuilder.Append("&exclude=").Append(_exclude);
                if (_startAfter != null)
                    pathBuilder.Append("&startAfter=").Append(Uri.EscapeDataString(_startAfter));
            }

            if (_includes != null)
            {
                foreach (var include in _includes)
                {
                    pathBuilder.Append("&include=").Append(include);
                }
            }

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            if (_id != null)
            {
                pathBuilder.Append("&id=").Append(Uri.EscapeDataString(_id));
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
                uniqueIds.ApplyIfNotNull(id => pathBuilder.Append("&id=").Append(Uri.EscapeDataString(id)));
            }
            else
            {
                var calculateHash = CalculateHash(context, uniqueIds);
                pathBuilder.Append("loadHash=").Append(calculateHash);
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

        private static ulong CalculateHash(JsonOperationContext ctx, HashSet<string> uniqueIds)
        {
            using (var hasher = new HashCalculator(ctx))
            {
                foreach (var x in uniqueIds)
                    hasher.Write(x);

                return hasher.GetHash();
            }
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

            Result = JsonDeserializationClient.GetDocumentsResult(response);
        }

        public override bool IsReadRequest => true;
    }
}
