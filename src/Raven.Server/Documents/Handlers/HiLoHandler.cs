// -----------------------------------------------------------------------
//  <copyright file="HiLoHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.HiLo;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class HiLoHandler : DatabaseRequestHandler
    {
        public const string RavenHiloIdPrefix = "Raven/Hilo/";

        [RavenAction("/databases/*/hilo/next", "GET", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task GetNextHiLo()
        {
            using (var processor = new HiLoHandlerProcessorForGetNextHiLo(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/hilo/return", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task ReturnHiLo()
        {
            using (var processor = new HiLoHandlerProcessorForReturnHiLo(this))
                await processor.ExecuteAsync();
        }
    }
}
