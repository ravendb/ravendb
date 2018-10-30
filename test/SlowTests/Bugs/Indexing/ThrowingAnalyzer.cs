//-----------------------------------------------------------------------
// <copyright file="ThrowingAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using FastTests;
using Xunit;
using System.Linq;
using System.Threading;
using Lucene.Net.Analysis;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;

namespace SlowTests.Bugs.Indexing
{
    public class ThrowingAnalyzer : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }

        [Fact]
        public void Should_give_clear_error()
        {
            var fieldOptions = new IndexFieldOptions { Analyzer = typeof(ThrowingAnalyzerImpl).AssemblyQualifiedName };

            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                                                new IndexDefinition
                                                {
                                                    Maps = { "from doc in docs select new { doc.Name}" },
                                                    Fields = { { "Name", fieldOptions } },
                                                    Name = "foo"
                                                }}));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Ayende" });
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    Assert.Throws<RavenException>(() =>

                        session.Query<User>("foo")
                        .Customize(x => x.WaitForNonStaleResults())
                            .ToList()
                    );
                }

                var db = GetDocumentDatabaseInstanceFor(store).Result;
                for (int i = 0; i < 10; i++)
                {
                    if (db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount()) > 0)
                        break;
                    Thread.Sleep(100);
                }
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                Assert.NotEqual(errorsCount, 0);
            }
        }

        [Fact]
        public void Should_disable_index()
        {
            var fieldOptions = new IndexFieldOptions { Analyzer = typeof(ThrowingAnalyzerImpl).AssemblyQualifiedName };

            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Maps = { "from doc in docs select new { doc.Name}" },
                        Fields = { { "Name", fieldOptions } },
                        Name = "foo"
                    }}));

                for (var i = 0; i < 20; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "Ayende" });
                        session.SaveChanges();
                    }

                    WaitForIndexing(store, allowErrors: true);
                }

                var fooIndex = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.First(x => x.Name == "foo");

                Assert.True(fooIndex.State == IndexState.Error);

                var db = GetDocumentDatabaseInstanceFor(store).Result;
                for (int i = 0; i < 10; i++)
                {
                    if (db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount()) > 0)
                        break;
                    Thread.Sleep(100);
                }
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                Assert.NotEqual(errorsCount, 0);

            }
        }

        private class ThrowingAnalyzerImpl : Analyzer
        {
            public ThrowingAnalyzerImpl()
            {
                throw new InvalidOperationException("oops");
            }

            public override TokenStream TokenStream(string fieldName, TextReader reader)
            {
                throw new NotImplementedException();
            }
        }
    }
}
