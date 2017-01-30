// -----------------------------------------------------------------------
//  <copyright file="AdminResourcesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class AdminResourcesHandler : RequestHandler
    {
        [RavenAction("/admin/*/disable", "POST", "/admin/{resourceType:databases|fs|cs|ts}/disable?name={resourceName:string|multiple}")]
        public Task DisableDatabases()
        {
            ToggleDisableDatabases(disableRequested: true);

            return Task.CompletedTask;
        }

        [RavenAction("/admin/*/enable", "POST", "/admin/{resourceType:databases|fs|cs|ts}/enable?name={resourceName:string|multiple}")]
        public Task EnableDatabases()
        {
            ToggleDisableDatabases(disableRequested: false);

            return Task.CompletedTask;
        }

        private void ToggleDisableDatabases(bool disableRequested)
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

            var names = GetStringValuesQueryString("name");

            var databasesToUnload = new List<string>();

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (var tx = context.OpenWriteTransaction())
            {
                writer.WriteStartArray();
                var first = true;
                foreach (var name in names)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    var dbId = resourcePrefix + name;
                    var dbDoc = ServerStore.Read(context, dbId);

                    if (dbDoc == null)
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Name"] = name,
                            ["Success"] = false,
                            ["Reason"] = "database not found",
                        });
                        continue;
                    }

                    object disabledValue;
                    if (dbDoc.TryGetMember("Disabled", out disabledValue))
                    {
                        if ((bool) disabledValue == disableRequested)
                        {
                            var state = disableRequested ? "disabled" : "enabled";
                            context.Write(writer, new DynamicJsonValue
                            {
                                ["Name"] = name,
                                ["Success"] = false,
                                ["Disabled"] = disableRequested,
                                ["Reason"] = $"Database already {state}",
                            });
                            continue;
                        }
                    }

                    dbDoc.Modifications = new DynamicJsonValue(dbDoc)
                    {
                        ["Disabled"] = disableRequested
                    };

                    var newDoc2 = context.ReadObject(dbDoc, dbId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    ServerStore.Write(context, dbId, newDoc2);
                    ServerStore.NotificationCenter.AddAfterTransactionCommit(ResourceChanged.Create(dbId, ResourceChangeType.Put), tx);

                    databasesToUnload.Add(name);

                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Name"] = name,
                        ["Success"] = true,
                        ["Disabled"] = disableRequested,
                    });
                }

                tx.Commit();

                writer.WriteEndArray();
            }

            foreach (var name in databasesToUnload)
            {
                /* Right now only database resource is supported */
                ServerStore.DatabasesLandlord.UnloadAndLock(name, () =>
                {
                    // empty by design
                });
            }
        }
    }
}