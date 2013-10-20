// -----------------------------------------------------------------------
//  <copyright file="ServerInstanceResponder.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Database.Server.Security;
using Raven.Json.Linq;

namespace Raven.Tests.Server.Runner.Responders
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Threading;

    using Raven.Database;
    using Raven.Database.Config;
    using Raven.Database.Extensions;
    using Raven.Database.Server;
    using Raven.Database.Server.Abstractions;
    using Raven.Server;

    public class ServerInstanceResponder : ResponderBase
    {
        protected readonly string DataDir = string.Format(@".\TestDatabase-{0}\", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"));

        private readonly IDictionary<int, RavenDbServer> servers;

        public ServerInstanceResponder()
        {
            this.servers = new Dictionary<int, RavenDbServer>();

            ClearDatabaseDirectory();
            Directory.CreateDirectory(DataDir);
        }

        public override string UrlPattern
        {
            get { return "^/servers(.*)"; }
        }

        public override string[] SupportedVerbs
        {
            get
            {
                return new[] { "PUT", "DELETE", "GET" };
            }
        }

        public override void Respond(IHttpContext context)
        {
            switch (context.Request.HttpMethod)
            {
                case "GET":
                    OnGet(context);
                    break;
                case "PUT":
                    OnPut(context);
                    break;
                case "DELETE":
                    OnDelete(context);
                    break;
            }
        }

        private void OnGet(IHttpContext context)
        {
            int port;
            if (!int.TryParse(context.Request.QueryString["port"], out port))
            {
                context.SetStatusToBadRequest();
                return;
            }

            string action = context.Request.QueryString["action"];

            if (string.IsNullOrEmpty(action))
            {
                context.SetStatusToBadRequest();
                return;
            }

            if (!servers.ContainsKey(port))
            {
                return;
            }

            var server = servers[port];

            switch (action.ToLowerInvariant())
            {
                case "waitforallrequeststocomplete":
                    SpinWait.SpinUntil(() => server.Server.HasPendingRequests == false, TimeSpan.FromMinutes(15));
                    break;
                default:
                    context.SetStatusToBadRequest();
                    return;
            }
        }

        private void OnPut(IHttpContext context)
        {
            var serverConfiguration = context.ReadJsonObject<ServerConfiguration>();
            if (serverConfiguration == null)
            {
                context.SetStatusToBadRequest();
                return;
            }

            var configuration = serverConfiguration.ConvertToRavenConfiguration();

            if (serverConfiguration.HasApiKey)
            {
                configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
                Authentication.EnableOnce();
            }

            configuration.PostInit();

            MaybeRemoveServer(configuration.Port);
            var server = CreateNewServer(configuration, context);

			if (serverConfiguration.UseCommercialLicense)
			{
				EnableAuthentication(server.SystemDatabase);
			}

            if (serverConfiguration.HasApiKey)
            {
                server.SystemDatabase.Put("Raven/ApiKeys/" + serverConfiguration.ApiKeyName, null, RavenJObject.FromObject(new ApiKeyDefinition
                {
                    Name = serverConfiguration.ApiKeyName,
                    Secret = serverConfiguration.ApiKeySecret,
                    Enabled = true,
                    Databases = new List<DatabaseAccess>
				{
					new DatabaseAccess {TenantId = "*", Admin = true},
					new DatabaseAccess {TenantId = Constants.SystemDatabase, Admin = true},
				}
                }), new RavenJObject(), null);
            }
        }

        private RavenDbServer CreateNewServer(RavenConfiguration configuration, IHttpContext context)
        {
            configuration.DataDirectory = Path.Combine(DataDir, configuration.Port.ToString());

            bool deleteData;
            bool.TryParse(context.Request.QueryString["deleteData"], out deleteData);

            if (configuration.RunInMemory == false && deleteData)
                IOExtensions.DeleteDirectory(configuration.DataDirectory);

            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(configuration.Port);
            var server = new RavenDbServer(configuration);

            servers.Add(configuration.Port, server);

            Console.WriteLine("Created a server (Port: {0}, RunInMemory: {1})", configuration.Port, configuration.RunInMemory);

            return server;
        }

        private void OnDelete(IHttpContext context)
        {
            int port;
            if (!int.TryParse(context.Request.QueryString["port"], out port))
            {
                context.SetStatusToBadRequest();
                return;
            }

            MaybeRemoveServer(port);
        }

        public override void Dispose()
        {
            foreach (var port in servers.Keys)
                servers[port].Dispose();

            GC.Collect(2);
            GC.WaitForPendingFinalizers();
            ClearDatabaseDirectory();
        }

        private void MaybeRemoveServer(int port)
        {
            if (!servers.ContainsKey(port))
                return;

            servers[port].Dispose();
            servers.Remove(port);

            Console.WriteLine("Deleted a server at: " + port);
        }

        private void ClearDatabaseDirectory()
        {
            bool isRetry = false;

            while (true)
            {
                try
                {
                    IOExtensions.DeleteDirectory(DataDir);
                    break;
                }
                catch (IOException)
                {
                    if (isRetry)
                        throw;

                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    isRetry = true;
                }
            }
        }

		public static void EnableAuthentication(DocumentDatabase database)
		{
			var license = GetLicenseByReflection(database);
			license.Error = false;
			license.Status = "Commercial";

			// rerun this startup task
			database.StartupTasks.OfType<AuthenticationForCommercialUseOnly>().First().Execute(database);
		}

		public static LicensingStatus GetLicenseByReflection(DocumentDatabase database)
		{
			var field = database.GetType().GetField("validateLicense", BindingFlags.Instance | BindingFlags.NonPublic);
			var validateLicense = field.GetValue(database);

			var currentLicenseProp = validateLicense.GetType().GetProperty("CurrentLicense", BindingFlags.Static | BindingFlags.Public);

			return (LicensingStatus)currentLicenseProp.GetValue(validateLicense, null);
		}
    }

    [Serializable]
    internal class ServerConfiguration
    {
        public ServerConfiguration()
        {
            this.Settings = new Dictionary<string, string>();
        }

        public int Port { get; set; }

        public bool RunInMemory { get; set; }

		public bool UseCommercialLicense { get; set; }

        public string ApiKeyName { get; set; }

        public string ApiKeySecret { get; set; }

        public IDictionary<string, string> Settings { get; set; }

        public bool HasApiKey { get { return !string.IsNullOrEmpty(ApiKeyName) && !string.IsNullOrEmpty(ApiKeySecret); } }

        public RavenConfiguration ConvertToRavenConfiguration()
        {
            var configuration = new RavenConfiguration
                                {
                                    Port = Port,
                                    RunInMemory = RunInMemory
                                };

            foreach (var key in Settings.Keys)
            {
                configuration.Settings.Add(key, Settings[key]);
            }

            return configuration;
        }
    }
}