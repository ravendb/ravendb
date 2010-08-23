using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Lucene.Net.Analysis;
using Raven.Database;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Client.Tests.Indexes
{
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

        private string entityName = "som\xC9";  // \xC9, \xC8 are both E characters with differing accents
        private string searchString = "som\xC8";
        private string analyzedName = "some";

        [Fact]
        public void custom_analyzer_folds_ascii()
        {
            var tokens = LuceneAnalyzerUtils.TokensFromAnalysis(new CustomAnalyzer(), entityName);

            Assert.Equal(analyzedName, tokens.Single().TermText());
        }

        public void with_index_and_single_entity(Action<IDocumentSession> action)
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("someIndex", new IndexDefinition()
                {
                    Map = "from doc in docs select new { Name = doc.Name }",
                    Analyzers = new Dictionary<string, string>()
                        {
                            {"Name", typeof(CustomAnalyzer).AssemblyQualifiedName}
                        }
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new Entity() { Name = entityName });
                    session.SaveChanges();
                }

                using (var session2 = store.OpenSession())
                {
                    action(session2);
                }
            }
        }

        [Fact]
        public void find_matching_document_with_lucene_query()
        {
            with_index_and_single_entity(delegate(IDocumentSession session)
                {

                    var result = session.LuceneQuery<Entity>("someIndex").WaitForNonStaleResults()
                        .WhereEquals("Name", searchString, true, false)
                        .ToArray();

                    Assert.Equal(1, result.Length);
                });
        }


        [Fact(Skip ="LINQ version isn't working")]
        public void find_matching_document_with_linq_query()
        {
            with_index_and_single_entity(delegate(IDocumentSession session)
            {
                var result = session.Query<Entity>("someIndex").Customize(a => a.WaitForNonStaleResults())
                    .Where(e => e.Name == searchString)
                    .ToArray();

                Assert.Equal(1, result.Length);
            });
        }
    }
}
