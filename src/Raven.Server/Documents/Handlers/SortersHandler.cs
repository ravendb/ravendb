using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Indexes.Sorting;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers
{
    public class SortersHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/sorters", "GET", AuthorizationStatus.ValidUser)]
        public Task Get()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                DatabaseRecord record;
                using (context.OpenReadTransaction())
                {
                    record = ServerStore.Cluster.ReadDatabase(context, Database.Name);
                }

                var sorters = record.Sorters;
                
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    
                    writer.WriteStartObject();

                    if (sorters != null)
                    {
                        writer.WriteArray(context, "Sorters", sorters.Values, (w, c, sorter) =>
                        {
                            w.WriteStartObject();
                            
                            w.WritePropertyName(nameof(SorterDefinition.Name));
                            w.WriteString(sorter.Name);
                            w.WriteComma();
                            
                            w.WritePropertyName(nameof(SorterDefinition.Code));
                            w.WriteString(sorter.Code);
                            
                            w.WriteEndObject();
                        });
                    }
                    
                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

    }
}
