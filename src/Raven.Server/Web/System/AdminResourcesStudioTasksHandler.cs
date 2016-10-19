// -----------------------------------------------------------------------
//  <copyright file="AdminResourcesStudioTasksHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class AdminResourcesStudioTasksHandler : RequestHandler
    {
        [RavenAction("/admin/*/toggle-disable", "POST", "/admin/{resourceType:databases|fs|cs|ts}/toggle-disable?name={resourceName:string|multiple}&isDisabled={isDisabled:bool}")]
        public Task PostToggleDisableDatabases()
        {
            var resourceType = RouteMatch.Url.Substring(RouteMatch.CaptureStart, RouteMatch.CaptureLength);
            string resourcePrefix;
            switch (resourceType)
            {
                case Constants.Database.UrlPrefix:
                    resourcePrefix = Constants.Database.Prefix;
                    break;
                case Constants.FileSystem.UrlPrefix:
                    resourcePrefix = Constants.FileSystem.Prefix;
                    break;
                case Constants.Counter.UrlPrefix:
                    resourcePrefix = Constants.Counter.Prefix;
                    break;
                case Constants.TimeSeries.UrlPrefix:
                    resourcePrefix = Constants.TimeSeries.Prefix;
                    break;
                default:
                    throw new InvalidOperationException($"Resource type is not valid: '{resourceType}'");
            }

            var names = HttpContext.Request.Query["name"];
            if (names.Count == 0)
                throw new ArgumentException("Query string \'name\' is mandatory, but wasn\'t specified");
            var disableRequested = GetBoolValueQueryString("disable").Value;

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartArray();
                var first = true;
                foreach (var name in names)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    var dbId = resourcePrefix + name;
                    BlittableJsonReaderObject dbDoc;
                    using (var tx = context.OpenReadTransaction())
                    {
                        dbDoc = ServerStore.Read(context, dbId);
                    }
                    if (dbDoc == null)
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["name"] = name,
                            ["success"] = false,
                            ["reason"] = "database not found",
                        });
                        continue;
                    }

                    object disabledValue;
                    var disabled = false;
                    if (dbDoc.TryGetMember("Disabled", out disabledValue))
                    {
                        disabled = (bool)disabledValue;
                    }

                    if (disabled == disableRequested)
                    {
                        var state = disableRequested ? "disabled" : "enabled";
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["name"] = name,
                            ["success"] = false,
                            ["disabled"] = disableRequested,
                            ["reason"] = $"Database already {state}",
                        });
                        continue;
                    }

                    var newDoc = new DynamicJsonValue(dbDoc);
                    newDoc.Properties.Enqueue(Tuple.Create("Disabled", (object) disableRequested));
                    var newDoc2 = context.ReadObject(newDoc, dbId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    /* Right now only database resource is supported */
                    ServerStore.DatabasesLandlord.UnloadAndLock(name, () =>
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            ServerStore.Write(context, dbId, newDoc2);
                            tx.Commit();
                        }
                    });

                    context.Write(writer, new DynamicJsonValue
                    {
                        ["name"] = name,
                        ["success"] = true,
                        ["disabled"] = disableRequested,
                    });
                }
                writer.WriteEndArray();
            }
            return Task.CompletedTask;
        }
    }
}