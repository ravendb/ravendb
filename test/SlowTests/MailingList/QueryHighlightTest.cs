using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.MailingList
{
    public class QueryHighlightTest : RavenTestBase
    {
        public QueryHighlightTest(ITestOutputHelper output) : base(output)
        {
        }

        private const string Q = "What words rhyme with concurrency and asymptotic?";

        [Theory]
        [SearchEngineInlineData(SearchEngineType.Lucene, Q, "con cur", "con cu")]
        [SearchEngineInlineData(SearchEngineType.Lucene, Q, "con ency", "con cy")]
        [SearchEngineInlineData(SearchEngineType.Lucene, Q, "curr ency", "curr enc")]
        [SearchEngineInlineData(SearchEngineType.Lucene, Q, "wo rds", "wor ds")]
        [SearchEngineInlineData(SearchEngineType.Lucene, Q, "asymp totic", "asymp tot")]
        [SearchEngineInlineData(SearchEngineType.Lucene, Q, "asymp totic", "asymp tic")]
        public void ShouldReturnResultsWithHighlightsAndThrowException(string searchEngineType, string question, string goodSearchTerm, string badSearchTerm)
        {
            using (var store = GetDocumentStore(Options.ForSearchEngine(searchEngineType)))
            {
                new QuestionIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Question
                    {
                        QuestionText = question
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Highlightings highlightings;

                    var goodResult = session.Advanced.DocumentQuery<Question, QuestionIndex>()
                        .WaitForNonStaleResults()
                        .Highlight(x => x.QuestionText, 100, 1, out highlightings)
                        .Search(x => x.QuestionText, goodSearchTerm)
                        .OrderByScore()
                        .FirstOrDefault();

                    Assert.Equal(goodResult.QuestionText, question);

                    session.Advanced.DocumentQuery<Question, QuestionIndex>()
                        .Highlight(x => x.QuestionText, 100, 1, out highlightings)
                        .Search(x => x.QuestionText, badSearchTerm)
                        .OrderByScore()
                        .FirstOrDefault();
                }
            }
        }

        private class Question
        {
            public string QuestionText { get; set; }
        }

        private class QuestionIndex : AbstractIndexCreationTask<Question>
        {
            public QuestionIndex()
            {
                Map = questions => from question in questions
                                   select new
                                   {
                                       question.QuestionText
                                   };

                Analyze(x => x.QuestionText, typeof(NGramAnalyzer).AssemblyQualifiedName);
                Store(x => x.QuestionText, FieldStorage.Yes);
                Index(x => x.QuestionText, FieldIndexing.Search);
                TermVector(x => x.QuestionText, FieldTermVector.WithPositionsAndOffsets);
            }
        }
    }
}
