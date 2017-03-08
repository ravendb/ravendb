// -----------------------------------------------------------------------
//  <copyright file="ServersController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Server.Runner.Data;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Web.Http;

namespace Raven.Tests.Server.Runner.Controllers
{
    public class ServersController : ApiController
    {
        [HttpPut]
        public async Task<IHttpActionResult> PutServer([FromUri]bool deleteData = false)
        {
            var json = await ReadJsonAsync();
            var serverConfiguration = json.JsonDeserialization<ServerConfiguration>();

            if (serverConfiguration == null)
                return BadRequest();

            var configuration = serverConfiguration.ConvertToRavenConfiguration();

            if (serverConfiguration.HasApiKey)
            {
                configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
                Authentication.EnableOnce();
            }

            configuration.PostInit();

            MaybeRemoveServer(configuration.Port);
            var server = CreateNewServer(configuration, deleteData);

            if (serverConfiguration.UseCommercialLicense)
                EnableAuthentication(server.SystemDatabase);

            if (serverConfiguration.HasApiKey)
            {
                server.SystemDatabase.Documents.Put("Raven/ApiKeys/" + serverConfiguration.ApiKeyName, null, RavenJObject.FromObject(new ApiKeyDefinition
                {
                    Name = serverConfiguration.ApiKeyName,
                    Secret = serverConfiguration.ApiKeySecret,
                    Enabled = true,
                    Databases = new List<ResourceAccess>
                {
                    new ResourceAccess {TenantId = "*", Admin = true},
                    new ResourceAccess {TenantId = Constants.SystemDatabase, Admin = true},
                }
                }), new RavenJObject(), null);
            }

            return Json(new { ServerUrl = configuration.ServerUrl });
        }

        [HttpGet]
        public IHttpActionResult GetServer([FromUri]int port, [FromUri]string action)
        {
            if (!Context.Servers.ContainsKey(port))
                return Ok();

            var server = Context.Servers[port];

            switch (action.ToLowerInvariant())
            {
                case "waitforallrequeststocomplete":
                    SpinWait.SpinUntil(() => server.Server.HasPendingRequests == false, TimeSpan.FromMinutes(15));
                    break;
            }

            return Ok();
        }

        [HttpDelete]
        public void DeleteServer([FromUri]int port)
        {
            MaybeRemoveServer(port);

            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        private static RavenDbServer CreateNewServer(InMemoryRavenConfiguration configuration, bool deleteData)
        {
            var port = configuration.Port.ToString(CultureInfo.InvariantCulture);

            configuration.DataDirectory = Path.Combine(Context.DataDir, port, "System");
            configuration.FileSystem.DataDirectory = Path.Combine(Context.DataDir, port, "FileSystem");
            configuration.AccessControlAllowOrigin = new HashSet<string> { "*" };
            configuration.MaxSecondsForTaskToWaitForDatabaseToLoad = 20;

            if (configuration.RunInMemory == false && deleteData)
            {
                var pathToDelete = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Context.DataDir, port);
                Context.DeleteDirectory(pathToDelete);
            }

            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(configuration.Port);
            var server = new RavenDbServer(configuration) { UseEmbeddedHttpServer = true };

            server.Initialize();
            Context.Servers.Add(configuration.Port, server);

            Console.WriteLine("Created a server (Port: {0}, RunInMemory: {1})", configuration.Port, configuration.RunInMemory);

            return server;
        }

        private static void MaybeRemoveServer(int port)
        {
            if (!Context.Servers.ContainsKey(port))
                return;

            Context.Servers[port].Dispose();
            Context.Servers.Remove(port);

            Console.WriteLine("Deleted a server at: " + port);
        }

        private static void EnableAuthentication(DocumentDatabase database)
        {
            var license = GetLicenseByReflection(database);
            license.Error = false;
            license.Status = "Commercial";

            // rerun this startup task
            database.StartupTasks.OfType<AuthenticationForCommercialUseOnly>().First().Execute(database);
        }

        private static LicensingStatus GetLicenseByReflection(DocumentDatabase database)
        {
            var field = database.GetType().GetField("initializer", BindingFlags.Instance | BindingFlags.NonPublic);
            var initializer = field.GetValue(database);
            var validateLicenseField = initializer.GetType().GetField("validateLicense", BindingFlags.Instance | BindingFlags.NonPublic);
            var validateLicense = validateLicenseField.GetValue(initializer);

            var currentLicenseProp = validateLicense.GetType().GetProperty("CurrentLicense", BindingFlags.Static | BindingFlags.Public);

            return (LicensingStatus)currentLicenseProp.GetValue(validateLicense, null);
        }

        private async Task<RavenJObject> ReadJsonAsync()
        {
            using (var stream = await Request.Content.ReadAsStreamAsync())
            using (var streamReader = new StreamReader(stream))
            using (var jsonReader = new RavenJsonTextReader(streamReader))
                return RavenJObject.Load(jsonReader);
        }
    }
}
