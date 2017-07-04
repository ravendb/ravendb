// -----------------------------------------------------------------------
//  <copyright file="SmugglerController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler;
using Raven.Database.Actions;
using Raven.Database.Extensions;
using Raven.Database.Server.Migrator;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Smuggler;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public class SmugglerController : BaseDatabaseApiController
    {
        [HttpPost]
        [RavenRoute("smuggler/export")]
        [RavenRoute("databases/{databaseName}/smuggler/export")]
        public HttpResponseMessage Export(ExportOptions options)
        {
            if (options == null)
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            var headers = CurrentOperationContext.Headers.Value;
            var user = CurrentOperationContext.User.Value;

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new PushStreamContent((stream, content, transportContext) => StreamToClient(stream, options, headers, user))
                {
                    Headers =
                    {
                        ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" }
                    }
                }
            };
        }

        private void StreamToClient(Stream stream, ExportOptions options, Lazy<NameValueCollection> headers, IPrincipal user)
        {
            var old = CurrentOperationContext.Headers.Value;
            var oldUser = CurrentOperationContext.User.Value;
            try
            {
                CurrentOperationContext.Headers.Value = headers;
                CurrentOperationContext.User.Value = user;

                Database.TransactionalStorage.Batch(accessor =>
                {
                    var bufferStream = new BufferedStream(stream, 1024 * 64);

                    using (var cts = new CancellationTokenSource())
                    using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
                    using (var streamWriter = new StreamWriter(bufferStream))
                    using (var writer = new JsonTextWriter(streamWriter))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Results");
                        writer.WriteStartArray();

                        var exporter = new SmugglerExporter(Database, options);

                        exporter.Export(item => WriteToStream(writer, item, timeout), cts.Token);

                        writer.WriteEndArray();
                        writer.WriteEndObject();
                        writer.Flush();
                        bufferStream.Flush();
                    }
                });
            }
            finally
            {
                CurrentOperationContext.Headers.Value = old;
                CurrentOperationContext.User.Value = oldUser;
            }
        }

        private static void WriteToStream(JsonWriter writer, RavenJObject item, CancellationTimeout timeout)
        {
            timeout.Delay();

            item.WriteTo(writer);
        }

        [HttpPost]
        [RavenRoute("migration")]
        [RavenRoute("databases/{databaseName}/migration")]
        public async Task<HttpResponseMessage> DatabaseMigrator()
        {
            MigrationDetails migrationDetails;
            try
            {
                migrationDetails = await ReadJsonObjectAsync<MigrationDetails>().ConfigureAwait(false);
            }
            catch (InvalidOperationException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize database migration request. Error: ", e);
                return GetMessageWithObject(new
                {
                    Message = "Could not understand json, please check its validity.",
                    Error = e.Message
                }, (HttpStatusCode)500);

            }
            catch (InvalidDataException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize database migration request. Error: ", e);

                return GetMessageWithObject(new
                {
                    Error = e
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
            }

            var migrateToV4 = new DatabaseMigrator(Database, migrationDetails, DatabasesLandlord.MaxIdleTimeForTenantDatabaseInSec);
            var cts = new CancellationTokenSource();
            var status = new MigrationStatus();
            var task = migrateToV4.Execute(status, cts);

            long id;
            Database.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.MigrateDatabase,
                Description = $"Migrating database named '{Database.Name}' to '{migrationDetails.Url}'"
            }, out id, cts);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
        }
    }
}