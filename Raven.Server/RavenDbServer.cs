//-----------------------------------------------------------------------
// <copyright file="RavenDbServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Owin.Hosting;
using Owin;
using Raven.Abstractions.Logging;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server.Tenancy;
using Raven.Server.Discovery;

namespace Raven.Server
{
    public class RavenDbServer : IDisposable
    {
        private static readonly ILog Logger = LogManager.GetCurrentClassLogger();
        private readonly DocumentDatabase database;
        private readonly IDisposable server;
        private readonly IServerThingsForTests serverThingsForTests;
        private ClusterDiscoveryHost discoveryHost;

        public RavenDbServer(InMemoryRavenConfiguration settings)
        {
            database = new DocumentDatabase(settings);
            try
            {
                database.SpinBackgroundWorkers();
                var options = new RavenDBOwinOptions(settings, database);
                //TODO use settings.ServerUrl
                server = WebApp.Start("http://+:" + settings.Port, app => app.UseRavenDB(options));
                serverThingsForTests = new ServerThingsForTests(options);
            }
            catch (Exception)
            {
                database.Dispose();
                database = null;

                throw;
            }

            ClusterDiscovery(settings);
        }

        public DocumentDatabase Database
        {
            get { return database; }
        }

        public IServerThingsForTests Server
        {
            get { return serverThingsForTests; }
        }

        public void Dispose()
        {
            server.Dispose();
            database.Dispose();
        }

        private void ClusterDiscovery(InMemoryRavenConfiguration settings)
        {
            if (settings.DisableClusterDiscovery == false)
            {
                discoveryHost = new ClusterDiscoveryHost();
                try
                {
                    discoveryHost.Start();
                    discoveryHost.ClientDiscovered += async (sender, args) =>
                    {
                        var httpClient = new HttpClient(new HttpClientHandler());
                        var values = new Dictionary<string, string>
                        {
                            {"Url", settings.ServerUrl},
                            {"ClusterName", settings.ClusterName},
                        };
                        try
                        {
                            HttpResponseMessage result =
                                await httpClient.PostAsync(args.ClusterManagerUrl, new FormUrlEncodedContent(values));
                            result.EnsureSuccessStatusCode();
                        }
                        catch (Exception e)
                        {
                            Logger.ErrorException(
                                "Cannot post notification for cluster discovert to: " + settings.ServerUrl, e);
                        }
                    };
                }
                catch (Exception e)
                {
                    discoveryHost.Dispose();
                    discoveryHost = null;

                    Logger.ErrorException("Cannot setup cluster discovery", e);
                }
            }
        }

        //TODO need a better name
        private class ServerThingsForTests : IServerThingsForTests
        {
            private readonly RavenDBOwinOptions options;

            public ServerThingsForTests(RavenDBOwinOptions options)
            {
                this.options = options;
            }

            public bool HasPendingRequests
            {
                get { return false; } //TODO: fix (copied from WebApiServer
            }

            public int NumberOfRequests
            {
                get { return options.Landlord.NumberOfRequests; }
            }

            public DatabasesLandlord Landlord
            {
                get { return options.Landlord; }
            }

            public void ResetNumberOfRequests()
            {
                options.Landlord.ResetNumberOfRequests();
            }

            public Task<DocumentDatabase> GetDatabaseInternal(string databaseName)
            {
                return options.Landlord.GetDatabaseInternal(databaseName);
            }
        }
    }

    //TODO need a better name
    public interface IServerThingsForTests
    {
        bool HasPendingRequests { get; }
        int NumberOfRequests { get; }
        DatabasesLandlord Landlord { get; }
        void ResetNumberOfRequests();
        Task<DocumentDatabase> GetDatabaseInternal(string databaseName);
    }
}