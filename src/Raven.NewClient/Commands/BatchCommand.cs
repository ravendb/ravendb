using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.NewClient.Client.Http;

namespace Raven.NewClient.Client.Commands
{
    public class BatchCommand : RavenCommand<BatchResult>
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

            string waitForReplicas;
            string waitForIndexes;

            OptionsToString(out waitForReplicas, out waitForIndexes);

            url = $"{node.Url}/databases/{node.Database}/bulk_docs";

            if (waitForReplicas != null)
                url += $"?{waitForReplicas}";

            if (waitForIndexes != null)
                url += $"{(waitForReplicas != null ? "&" : "?")}{waitForIndexes}";

            IsReadRequest = false;

            return request;
        }


        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
                throw new InvalidOperationException("Got null response from the server after doing a batch, something is very wrong. Probably a garbled response. " +
                                                         "Commands: " + string.Join(",", Commands));
            Result = JsonDeserializationClient.BatchResult(response);
        }

        private void OptionsToString(out string waitForReplicas, out string waitForIndexes)
        {
            waitForReplicas = null;
            waitForIndexes = null;

            if (Options != null)
            {
                if (Options.WaitForReplicas)
                    waitForReplicas =
                        $"waitForReplication=true&numberOfReplicasToWaitFor={Options.NumberOfReplicasToWaitFor}&waitForReplicasTimeout={Options.WaitForReplicasTimeout}" +
                        $"&throwOnTimeoutInWaitForReplicas={Options.ThrowOnTimeoutInWaitForReplicas}&majority={(Options.Majority ? "majority" : "exact")}";

                if (Options.WaitForIndexes)
                    waitForIndexes =
                        $"waitForIndexes=true&waitForIndexesTimeout={Options.WaitForIndexesTimeout}&waitForIndexThrow={Options.ThrowOnTimeoutInWaitForIndexes}" +
                        $"{(Options.WaitForSpecificIndexes != null ? "&waitForSpecificIndexs=" + string.Join("&waitForSpecificIndexs=", Options.WaitForSpecificIndexes) : "")}";
            }
        }
    }
}