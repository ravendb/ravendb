using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Lucene.Net.Analysis;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Client.Tests.Indexes
{
	[CLSCompliant(false)]
	public class CustomAnalyzer : KeywordAnalyzer
    {
        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            return new LowerCaseFilter(new ASCIIFoldingFilter(base.TokenStream(fieldName, reader)));
        }
    }

    public class UsingCustomLuceneAnalyzer : LocalClientTest
    {
        public class Entity
        {
            public string Name;
        }

        public class EntityCount
        {
            public string Name;
            public int Count;
        }

        private string entityName = "som\xC9";  // \xC9, \xC8 are both E characters with differing accents
        private string searchString = "som\xC8";
        private string analyzedName = "some";

        [Fact]
        public void custom_analyzer_folds_ascii()
        {
            var tokens = LuceneAnalyzerUtils.TokensFromAnalysis(new CustomAnalyzer(), entityName);

            Assert.Equal(analyzedName, tokens.Single());
        }
        
        public void with_index_and_some_entities(Action<IDocumentSession> action)
        {
            using (var store = NewDocumentStore())
            {
                var indexDefinition = new IndexDefinition<Entity, EntityCount>()
                {
                    Map = docs => docs.Select(doc => new { Name = doc.Name, NormalizedName = doc.Name, Count = 1 }),
                    Reduce = docs => from doc in docs
                                     group doc by new { doc.Name } into g
                                     select new { Name = g.Key.Name, NormalizedName = g.Key.Name, Count = g.Sum(c => c.Count) },
                    Indexes =
                        {
                            {e => e.Name, FieldIndexing.NotAnalyzed }
                        }
                }.ToIndexDefinition(store.Conventions);

                indexDefinition.Analyzers = new Dictionary<string, string>()
                {
                    {"NormalizedName", typeof (CustomAnalyzer).AssemblyQualifiedName}
                };

                store.DatabaseCommands.PutIndex("someIndex", indexDefinition);

                using (var session = store.OpenSession())
                {
                    session.Store(new Entity() { Name = entityName });
                    session.Store(new Entity() { Name = entityName });
                    session.Store(new Entity() { Name = entityName });
                    session.Store(new Entity() { Name = entityName });
                    session.Store(new Entity() { Name = "someOtherName1" });
                    session.Store(new Entity() { Name = "someOtherName2" });
                    session.Store(new Entity() { Name = "someOtherName3" });
                    session.SaveChanges();
                }
                
                // This wait should update the index with all changes...
                WaitForIndex(store, "someIndex");

                using (var session2 = store.OpenSession())
                {
                    action(session2);
                }
            }
        }

        [Fact]
        public void find_matching_document_with_lucene_query_and_redundant_wait()
        {
            with_index_and_some_entities(delegate(IDocumentSession session)
            {
                var result = session.Advanced.LuceneQuery<EntityCount>("someIndex").WaitForNonStaleResults()
                    .WhereEquals("NormalizedName", searchString, true, false)
                    .ToArray();

                Assert.Equal(1, result.Length);
                Assert.Equal(4, result.First().Count);
            });
        }

        [Fact]
        public void find_matching_document_with_lucene_query_and_without_redundant_wait()
        {
            with_index_and_some_entities(delegate(IDocumentSession session)
            {
                var result = session.Advanced.LuceneQuery<EntityCount>("someIndex")
                    .WhereEquals("NormalizedName", searchString, true, false)
                    .ToArray();

                Assert.Equal(1, result.Length);
                Assert.Equal(4, result.First().Count);
            });
        }

        protected void WaitForIndex(IDocumentStore store, string indexName)
        {
            using (var session = store.OpenSession())
            {
                //doesn't matter what the query is here, just want to see if it's stale or not
                session.Advanced.LuceneQuery<object>(indexName)
            		.Where("")
            		.WaitForNonStaleResultsAsOfNow(TimeSpan.FromSeconds(5))
            		.ToArray();

            }
        }
    }
}
