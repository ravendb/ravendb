using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Reflection;
using System.Text;
using Raven.Abstractions.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.NewClient.Client.Documents.Commands;
using Raven.NewClient.Client.Http;

namespace Raven.NewClient.Client.Documents.Commands
{
    public class BatchCommand : RavenCommand<BatchResult>
    {
        public JsonOperationContext Context;
        public List<DynamicJsonValue> Commands;
        public List<object> Entities;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
            };

            request.Content = new BlittableJsonContent(stream =>
            {
                using (var writer = new BlittableJsonTextWriter(Context, stream))
                {
                    writer.WriteStartArray();
                    bool first = true;
                    foreach (var command in Commands)
                    {
                        if (!(first))
                            writer.WriteComma();
                        first = false;
                        Context.Write(writer, command);
                    }
                    writer.WriteEndArray();
                }
            });

            url = "bulk_docs";
            return request;
        }


        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
                throw new InvalidOperationException("Got null response from the server after doing a batch, something is very wrong. Probably a garbled response. " +
                                                         "Commands: " + string.Join(",", Commands));
            Result = JsonDeserializationClient.BatchResult(response);
        }
    }
}