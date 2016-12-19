//-----------------------------------------------------------------------
// <copyright file="ThrowingAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Lucene.Net.Analysis;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;
using Xunit;
using System.Linq;
using Raven.Abstractions.Data;

namespace Raven.Tests.Bugs.Indexing
{
    public class ThrowingAnalyzer : RavenTest
    {
        [Fact]
        public void Should_give_clear_error()
        {
            using(var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("foo",
                                                new IndexDefinition
                                                {
                                                    Map = "from doc in docs select new { doc.Name}",
                                                    Analyzers = { { "Name", typeof(ThrowingAnalyzerImpl).AssemblyQualifiedName } }
                                                });

                using(var session = store.OpenSession())
                {
                    session.Store(new User{Name="Ayende"});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User>("foo")
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                }

                Assert.NotEmpty(store.SystemDatabase.Statistics.Errors);
            }
        }

        [Fact]
        public void Should_disable_index()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("foo",
                                                new IndexDefinition
                                                {
                                                    Map = "from doc in docs select new { doc.Name}",
                                                    Analyzers = { { "Name", typeof(ThrowingAnalyzerImpl).AssemblyQualifiedName } }
                                                });

                for (var i = 0; i < 20; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "Ayende" });
                        session.SaveChanges();
                    }

                    WaitForIndexing(store);
                }

                var index = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.Name == "foo");
                Assert.True(index.Priority == IndexingPriority.Disabled);

                Assert.NotEmpty(store.SystemDatabase.Statistics.Errors);
            }
        }

        public class ThrowingAnalyzerImpl : Analyzer
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
