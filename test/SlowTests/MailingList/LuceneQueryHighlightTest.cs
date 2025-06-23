using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class LuceneQueryHighlightTest : RavenTestBase
    {
        public LuceneQueryHighlightTest(ITestOutputHelper output) : base(output)
        {
        }

        private const string Q = "What words rhyme with concurrency and asymptotic?";

        [RavenTheory(RavenTestCategory.Lucene | RavenTestCategory.Highlighting | RavenTestCategory.Querying)]
        [RavenData(Q, "con cur", "con cu", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(Q, "con ency", "con cy", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(Q, "curr ency", "curr enc", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(Q, "wo rds", "wor ds", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(Q, "asymp totic", "asymp tot", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        [RavenData(Q, "asymp totic", "asymp tic", SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void ShouldReturnResultsWithHighlightsAndThrowException(Options options, string question, string goodSearchTerm, string badSearchTerm)
        {
            using (var store = GetDocumentStore(options))
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
