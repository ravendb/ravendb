//-----------------------------------------------------------------------
// <copyright file="CreatingIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Http;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Bugs.MultiTenancy
{
    public class CreatingIndexes : RemoteClientTest, IDisposable
    {
        protected RavenDbServer GetNewServer(int port)
        {
            return
                new RavenDbServer(new RavenConfiguration
                {
                    Port = port,
                    RunInMemory = true,
                    DataDirectory = "Data",
                    AnonymousUserAccessMode = AnonymousUserAccessMode.All
                });
        }


        [Fact]
        public void Mulitenancy_Test()
        {
            using (GetNewServer(8080))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "Test"
            }.Initialize())
            {
                store.DatabaseCommands.PutIndex("TestIndex",
                                                new IndexDefinition<Test, Test>
                                                {
                                                    Map = movies => from movie in movies
                                                                    select new {movie.Name}
                                                });

                using (var sess = store.OpenSession())
                {
                    sess.Store(new Test {Name = "xxx"});

                    sess.SaveChanges();
                }

                using (var sess = store.OpenSession())
                {
                    var result = sess.Query<Test>("TestIndex")
                        .Customize(x=>x.WaitForNonStaleResults())
                        .Where(x => x.Name == "xxx")
                        .FirstOrDefault();

                    Assert.NotNull(result);
                }
            }
        }

        public void Dispose()
        {
            IOExtensions.DeleteDirectory("Data");
            IOExtensions.DeleteDirectory("Test");
        }
    }
}
