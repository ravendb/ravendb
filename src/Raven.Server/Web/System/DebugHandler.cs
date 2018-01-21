using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Migration;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Web.System
{
    public sealed class DebugHandler : RequestHandler
    {

        [RavenAction("/debug/routes", "GET", AuthorizationStatus.ValidUser)]
        public Task Routes()
        {
            var debugRoutes = Server.Router.AllRoutes
                .Where(x => x.IsDebugInformationEndpoint)
                .GroupBy(x => x.Path)
                .OrderBy(x => x.Key);

            var productionRoutes = Server.Router.AllRoutes
              .Where(x => x.IsDebugInformationEndpoint == false)
                .GroupBy(x => x.Path)
                .OrderBy(x => x.Key);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Debug");
                writer.WriteStartArray();
                var first = true;
                foreach (var route in debugRoutes)
                {
                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;

                    writer.WriteStartObject();
                    writer.WritePropertyName("Path");
                    writer.WriteString(route.Key);
                    writer.WriteComma();
                    writer.WritePropertyName("Methods");
                    writer.WriteString(string.Join(", ", route.Select(x => x.Method)));
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();

                writer.WriteComma();
                writer.WritePropertyName("Production");
                writer.WriteStartArray();
                first = true;
                foreach (var route in productionRoutes)
                {
                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;

                    writer.WriteStartObject();
                    writer.WritePropertyName("Path");
                    writer.WriteString(route.Key);
                    writer.WriteComma();
                    writer.WritePropertyName("Methods");
                    writer.WriteString(string.Join(", ", route.Select(x => x.Method)));
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }
    }
}
