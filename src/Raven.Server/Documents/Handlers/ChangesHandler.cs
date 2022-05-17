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
            using (var processor = new ChangesHandlerProcessorForGetConnectionsDebugInfo(this))
                await processor.ExecuteAsync();
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
