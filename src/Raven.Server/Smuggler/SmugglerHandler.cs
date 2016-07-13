// -----------------------------------------------------------------------
//  <copyright file="SmugglerHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Smuggler
{
    public class SmugglerHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/smuggler/export", "POST")]
        public Task PostExport()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                new DatabaseDataExporter(Database).Export(context, ResponseBodyStream());
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/smuggler/import", "POST")]
        public Task PostImport()
        {
            return Task.CompletedTask;
        }
    }
}