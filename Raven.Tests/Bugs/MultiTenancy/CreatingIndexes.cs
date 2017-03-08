//-----------------------------------------------------------------------
// <copyright file="CreatingIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Server;
using Raven.Tests.Common;

using Xunit;
using Raven.Client.Extensions;

namespace Raven.Tests.Bugs.MultiTenancy
{
    public class CreatingIndexes : RavenTest
    {
        [Fact]
        public void Multitenancy_Test()
        {
            using (GetNewServer(8079))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8079",
                DefaultDatabase = "Test"
            }.Initialize())
            {
                store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("Test");
                store.DatabaseCommands.PutIndex("TestIndex",
                                                new IndexDefinitionBuilder<Test, Test>("TestIndex")
                                                {
                                                    Map = movies => from movie in movies
                                                                    select new {movie.Name}
                                                });

                using (var session = store.OpenSession())
                {
                    session.Store(new Test {Name = "xxx"});

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<Test>("TestIndex")
                        .Customize(x=>x.WaitForNonStaleResults())
                        .Where(x => x.Name == "xxx")
                        .FirstOrDefault();

                    Assert.NotNull(result);
                }
            }
        }

        public override void Dispose()
        {
            IOExtensions.DeleteDirectory("Data");
            IOExtensions.DeleteDirectory("Test");
            base.Dispose();
        }
    }
}
