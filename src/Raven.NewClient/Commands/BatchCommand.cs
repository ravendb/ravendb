using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.NewClient.Client.Http;

namespace Raven.NewClient.Client.Commands
{
    public class BatchCommand : RavenCommand<BlittableArrayResult>
    {
        public JsonOperationContext Context;
        public List<DynamicJsonValue> Commands;
        public BatchOptions Options { get; set; }

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

            var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/bulk_docs");

            AppendOptions(sb);


            IsReadRequest = false;

            url = sb.ToString();

            return request;
        }


        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
                throw new InvalidOperationException("Got null response from the server after doing a batch, something is very wrong. Probably a garbled response. " +
                                                         "Commands: " + string.Join(",", Commands));
            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }

        private void AppendOptions(StringBuilder sb)
        {
            if (Options == null)
                return;

            sb.AppendLine("?");

            if (Options.WaitForReplicas)
            {
                sb.Append("&waitForReplicasTimeout=").Append(Options.WaitForReplicasTimeout);
                if (Options.ThrowOnTimeoutInWaitForReplicas)
                {
                    sb.Append("&throwOnTimeoutInWaitForReplicas=true");
                }
                sb.Append("&numberOfReplicasToWaitFor=");

                sb.Append(Options.Majority
                    ? "majority"
                    : Options.NumberOfReplicasToWaitFor.ToString());
            }

            if (Options.WaitForIndexes)
            {
                sb.Append("&waitForIndexesTimeout=").Append(Options.WaitForIndexesTimeout);
                if (Options.ThrowOnTimeoutInWaitForIndexes)
                {
                    sb.Append("&waitForIndexThrow=true");
                }
                if (Options.WaitForSpecificIndexes != null)
                {
                    foreach (var specificIndex in Options.WaitForSpecificIndexes)
                    {
                        sb.Append("&waitForSpecificIndexs=").Append(specificIndex);
                    }
                }
            }
        }
    }
}