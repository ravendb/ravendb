using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.NewClient.Client.Http;

namespace Raven.NewClient.Client.Commands
{
    public class MultiGetCommand : RavenCommand<BlittableArrayResult>
    {
        public JsonOperationContext Context;
        public List<GetRequest> GetCommands;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
            };

            var commands = new List<DynamicJsonValue>();
            foreach (var req in GetCommands)
            {
                var headers = new DynamicJsonValue();
                foreach (var header in req.Headers)
                {
                    headers[header.Key] = header.Value;
                }
                commands.Add(new DynamicJsonValue()
                {
                    ["Url"] = $"/databases/{node.Database}{req.Url}",
                    ["Query"] = req.Query,
                    ["Method"] = req.Method,
                    ["Headers"] = headers,
                    ["Content"] = req.Content
                });
            }

            request.Content = new BlittableJsonContent(stream =>
            {
                using (var writer = new BlittableJsonTextWriter(Context, stream))
                {
                    writer.WriteStartArray();
                    bool first = true;
                    foreach (var command in commands)
                    {
                        if (!(first))
                            writer.WriteComma();
                        first = false;
                        Context.Write(writer, command);
                    }
                    writer.WriteEndArray();
                }
            });

            var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/multi_get");

            IsReadRequest = false;

            url = sb.ToString();

            return request;
        }


        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
                throw new InvalidOperationException();
            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }
    }
}