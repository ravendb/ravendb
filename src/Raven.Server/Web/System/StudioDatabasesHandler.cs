using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System;

public class StudioDatabasesHandler : RequestHandler
{
    [RavenAction("/studio-tasks/databases", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task Databases()
    {
        var databaseName = GetStringQueryString("name", required: false);
        var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            IEnumerable<RawDatabaseRecord> items;
            if (string.IsNullOrEmpty(databaseName) == false)
            {
                var item = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName);
                if (item == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                items = new[] { item };
            }
            else
            {
                items = ServerStore.Cluster.GetAllRawDatabases(context, GetStart(), GetPageSize());
            }

            var allowedDbs = await GetAllowedDbsAsync(null, requireAdmin: false, requireWrite: false);

            if (allowedDbs.HasAccess == false)
                return;

            if (allowedDbs.AuthorizedDatabases != null)
                items = items.Where(item => allowedDbs.AuthorizedDatabases.ContainsKey(item.DatabaseName));

            await using var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream());

            writer.WriteStartObject();

            writer.WriteArray(context, "Databases", items, (w, c, i) =>
            {
                w.WriteStartObject();
                {
                    w.WritePropertyName(nameof(i.DatabaseName));
                    w.WriteString(i.DatabaseName);

                    if (namesOnly)
                    {
                        w.WriteEndObject();
                        return;
                    }

                    w.WriteComma();

                    w.WritePropertyName(nameof(i.IsDisabled));
                    w.WriteBool(i.IsDisabled);
                    w.WriteComma();

                    w.WritePropertyName(nameof(i.IsEncrypted));
                    w.WriteBool(i.IsEncrypted);
                    w.WriteComma();

                    w.WritePropertyName(nameof(i.LockMode));
                    w.WriteString(i.LockMode.ToString());
                    w.WriteComma();

                    w.WritePropertyName(nameof(i.Topology));
                    var topology = i.Topology?.ToJson();
                    if (topology == null)
                        w.WriteNull();
                    else
                        c.Write(w, topology);

                    w.WriteComma();

                    var sharding = i.Sharding;
                    w.WritePropertyName(nameof(i.Sharding));
                    if (sharding == null)
                        w.WriteNull();
                    else
                    {
                        w.WriteStartObject();

                        w.WritePropertyName(nameof(sharding.Orchestrator));
                        {
                            w.WriteStartObject();

                            w.WritePropertyName(nameof(sharding.Orchestrator.Topology));
                            c.Write(w, sharding.Orchestrator.Topology.ToJson());

                            w.WriteEndObject();
                        }
                        w.WriteComma();

                        w.WritePropertyName(nameof(sharding.Shards));
                        {
                            w.WriteStartObject();

                            var index = 0;
                            foreach (var kvp in sharding.Shards)
                            {
                                if (index > 0)
                                    w.WriteComma();

                                w.WritePropertyName(kvp.Key.ToString());
                                c.Write(w, kvp.Value.ToJson());

                                index++;
                            }

                            w.WriteEndObject();
                        }

                        w.WriteEndObject();
                    }
                }
                w.WriteEndObject();
            });

            writer.WriteEndObject();
        }
    }
}
