//-----------------------------------------------------------------------
// <copyright file="AuthorizationTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections;
using System.ComponentModel.Composition.Hosting;
using System.Web;

using Raven.Bundles.Authorization;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Server.WebApi;
using Raven.Server;
using Raven.Tests.Common;

namespace Raven.Tests.Bundles.Authorization
{
    public abstract class AuthorizationTest : RavenTest
    {
        protected const string UserId = "Authorization/Users/Ayende";
        protected readonly DocumentStore store;
        protected readonly RavenDbServer server;

        protected readonly string DatabaseName = Raven.Abstractions.Data.Constants.SystemDatabase;
        
        protected AuthorizationTest()
        {
            RouteCacher.ClearCache();

            server = GetNewServer(activeBundles: "Authorization", configureConfig: 
                configuration => configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(AuthorizationDecisions).Assembly)));
            store = NewRemoteDocumentStore(ravenDbServer: server,  databaseName: DatabaseName);
            
            foreach (DictionaryEntry de in HttpRuntime.Cache)
            {
                HttpRuntime.Cache.Remove((string)de.Key);
            }
        }

        protected DocumentDatabase Database
        {
            get
            {
                return server.Server.GetDatabaseInternal(DatabaseName).Result;
            }
        }
    }
}
