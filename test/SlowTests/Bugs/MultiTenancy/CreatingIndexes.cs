//-----------------------------------------------------------------------
// <copyright file="CreatingIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Bugs.MultiTenancy
{
    public class CreatingIndexes : RavenTestBase
    {

        [Fact]
        public void Multitenancy_Test()
        {

            DoNotReuseServer();
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore => documentStore.Database = "Test"
            }))
            {
                Assert.Equal("Test", store.Database);

                var doc = new DatabaseRecord("Test");
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

                var indexDefinition = new IndexDefinitionBuilder<Test, Test>("TestIndex")
                                                        {
                                                            Map = movies => from movie in movies
                                                                            select new { movie.Name }
                }.ToIndexDefinition(new DocumentConventions());
                indexDefinition.Name = "TestIndex";
                store.Maintenance.ForDatabase("Test").Send(new PutIndexesOperation(new[] {indexDefinition}));

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
