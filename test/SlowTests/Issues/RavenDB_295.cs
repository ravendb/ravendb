// -----------------------------------------------------------------------
//  <copyright file="RavenDB_295.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_295 : RavenTestBase
    {
        public RavenDB_295(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUpdateSuggestions()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "john" });
                    session.Store(new { Name = "darsy" });
                    session.SaveChanges();
                }
                store.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Name = "test",
                        Maps = new HashSet<string> { "from doc in docs select new { doc.Name}" },
                        Fields = new Dictionary<string, IndexFieldOptions>
                        {
                            {
                                "Name",
                                new IndexFieldOptions {Suggestions = true}
                            }
                        }
                    }}));

                Indexes.WaitForIndexing(store);

                //var suggestionQueryResult = store.DatabaseCommands.Suggest("test", new SuggestionQuery
                //{
                //    Field = "Name",
                //    Term = "orne"
                //});
                //Assert.Empty(suggestionQueryResult.Suggestions);

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "oren" });
                    session.SaveChanges();
                }
                Indexes.WaitForIndexing(store);

                //suggestionQueryResult = store.DatabaseCommands.Suggest("test", new SuggestionQuery
                //{
                //    Field = "Name",
                //    Term = "orne"
                //});
                //Assert.NotEmpty(suggestionQueryResult.Suggestions);
            }
        }

        [Fact]
        public void CanUpdateSuggestions_AfterRestart()
        {
            var dataDir = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = dataDir
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "john" });
                    session.Store(new { Name = "darsy" });
                    session.SaveChanges();
                }
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Name = "test",
                    Maps = new HashSet<string> { "from doc in docs select new { doc.Name}" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        {
                            "Name",
                            new IndexFieldOptions {Suggestions = true}
                        }
                    }
                }}));

                Indexes.WaitForIndexing(store);

                //var suggestionQueryResult = store.DatabaseCommands.Suggest("test", new SuggestionQuery
                //{
                //    Field = "Name",
                //    Term = "jhon"
                //});
                //Assert.NotEmpty(suggestionQueryResult.Suggestions);
            }

            using (var store = GetDocumentStore(new Options
            {
                Path = dataDir
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "oren" });
                    session.SaveChanges();
                }
                Indexes.WaitForIndexing(store);

                //var suggestionQueryResult = store.DatabaseCommands.Suggest("test", new SuggestionQuery
                //{
                //    Field = "Name",
                //    Term = "jhon"
                //});
                //Assert.NotEmpty(suggestionQueryResult.Suggestions);

                //suggestionQueryResult = store.DatabaseCommands.Suggest("test", new SuggestionQuery
                //{
                //    Field = "Name",
                //    Term = "orne"
                //});
                //Assert.NotEmpty(suggestionQueryResult.Suggestions);
            }
        }
    }
}
