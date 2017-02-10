//-----------------------------------------------------------------------
// <copyright file="CreatingIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Client.Operations.Databases;
using Raven.Client.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Bugs.MultiTenancy
{
    public class CreatingIndexes : RavenNewTestBase
    {

        [Fact]
        public void Multitenancy_Test()
        {

            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                var doc = MultiDatabase.CreateDatabaseDocument("Test");
                store.Admin.Send(new CreateDatabaseOperation(doc));
                store.DefaultDatabase = "Test";

                store.Admin.ForDatabase("Test").Send(new PutIndexOperation("TestIndex",
                                                        new IndexDefinitionBuilder<Test, Test>("TestIndex")
                                                        {
                                                            Map = movies => from movie in movies
                                                                            select new { movie.Name }
                                                        }.ToIndexDefinition(new DocumentConvention())));

                using (var session = store.OpenSession())
                {
                    session.Store(new Test { Name = "xxx" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<Test>("TestIndex")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "xxx")
                        .FirstOrDefault();

                    Assert.NotNull(result);
                }
            }
        }

        private class Test
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
