// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Changes;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class ChangesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/changes", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetChanges()
        {
            using (var processor = new ChangesHandlerProcessorForGetChanges(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/changes/debug", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetConnectionsDebugInfo()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Connections");

                writer.WriteStartArray();
                var first = true;
                foreach (var connection in Database.Changes.Connections)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;
                    context.Write(writer, connection.Value.GetDebugInfo());
                }
                writer.WriteEndArray();

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/changes", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task DeleteConnections()
        {
            var ids = GetStringValuesQueryString("id");

            foreach (var idStr in ids)
            {
                if (long.TryParse(idStr, NumberStyles.Any, CultureInfo.InvariantCulture, out long id) == false)
                    throw new ArgumentException($"Could not parse query string 'id' header as int64, value was: {idStr}");

                Database.Changes.Disconnect(id);
            }

            return NoContent();
        }
    }
}
