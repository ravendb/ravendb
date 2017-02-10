using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class GetDocumentCommand : RavenCommand<GetDocumentResult>
    {
        public string Id;

        public string[] Ids;
        public string[] Includes;

        public string Transformer;
        public Dictionary<string, object> TransformerParameters;

        public bool MetadataOnly;

        public string StartWith;
        public string Matches;
        public int Start;
        public int PageSize;
        public string Exclude;
        public RavenPagingInformation PagingInformation;
        public string SkipAfter;

        public JsonOperationContext Context;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder("docs?");
            
            if (MetadataOnly)
                pathBuilder.Append("&metadata-only=true");

            if (StartWith != null)
            {
                var actualStart = Start;

                var nextPage = PagingInformation != null && PagingInformation.IsForPreviousPage(Start, PageSize);
                if (nextPage)
                    actualStart = PagingInformation.NextPageStart;

                pathBuilder.Append($"startsWith={Uri.EscapeDataString(StartWith)}&start={actualStart.ToInvariantString()}&pageSize={PageSize.ToInvariantString()}");

                if (Matches != null)
                    pathBuilder.Append($"&matches={Matches}");
                if (Exclude != null)
                    pathBuilder.Append($"&exclude={Exclude}");
                if (nextPage)
                    pathBuilder.Append("&next-page=true");
            }

            if (Includes != null)
            {
                foreach (var include in Includes)
                {
                    pathBuilder.Append($"&include={include}");
                }
            }

            if (string.IsNullOrEmpty(Transformer) == false)
                pathBuilder.Append($"&transformer={Transformer}");

            if (TransformerParameters != null)
            {
                foreach (var tp in TransformerParameters)
                {
                    pathBuilder.Append($"&tp-{tp.Key}={tp.Value}");
                }
            }

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            if (Id != null)
            {
                pathBuilder.Append($"&id={Uri.EscapeDataString(Id)}");
            }
            else if (Ids != null)
            {
                PrepareRequestWithMultipleIds(pathBuilder, request, Ids, Context);
            }

            url = $"{node.Url}/databases/{node.Database}/" + pathBuilder;
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
                            writer.WriteStartArray();
                            bool first = true;
                            foreach (var id in uniqueIds)
                            {
                                if(!first)
                                    writer.WriteComma();
                                first = false;
                                writer.WriteString(id);
                            }
                            writer.WriteEndArray();
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