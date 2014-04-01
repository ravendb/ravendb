using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Analyzers;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.MailingList
{
	public class LuceneQueryHighlightTest : RavenTestBase
	{
		const string question = "What words rhyme with concurrency and asymptotic?";

		[Theory,
		InlineData(question, "con cur", "con cu"),
		InlineData(question, "con ency", "con cy"),
		InlineData(question, "curr ency", "curr enc"),
		InlineData(question, "wo rds", "wor ds"),
		InlineData(question, "asymp totic", "asymp tot"),
		InlineData(question, "asymp totic", "asymp tic")]
		public void ShouldReturnResultsWithHighlightsAndThrowException(string question, string goodSearchTerm, string badSearchTerm)
		{
			using (var store = NewDocumentStore())
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
					FieldHighlightings highlightings;

                    var goodResult = session.Advanced.DocumentQuery<Question, QuestionIndex>()
						.WaitForNonStaleResultsAsOfNow()
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

		class Question
		{
			public string QuestionText { get; set; }
		}

		class QuestionIndex : AbstractIndexCreationTask<Question>
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
				Index(x => x.QuestionText, FieldIndexing.Analyzed);
				TermVector(x => x.QuestionText, FieldTermVector.WithPositionsAndOffsets);
			}
		}
	}
}
