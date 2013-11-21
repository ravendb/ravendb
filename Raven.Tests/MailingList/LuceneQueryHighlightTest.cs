using System.IO;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Analysis.Tokenattributes;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Raven.Tests.Bugs;
using Raven.Tests.Helpers;
using System;
using System.Linq;
using Xunit;
using Xunit.Extensions;
using Version = Lucene.Net.Util.Version;

namespace RavenDB.Tests
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

					var goodResult = session.Advanced.LuceneQuery<Question, QuestionIndex>()
						.WaitForNonStaleResultsAsOfNow()
						.Highlight(x => x.QuestionText, 100, 1, out highlightings)
						.Search(x => x.QuestionText, goodSearchTerm)
						.OrderByScore()
						.FirstOrDefault();

					Assert.Equal(goodResult.QuestionText, question);

					Assert.Throws<ArgumentOutOfRangeException>(() =>
						session.Advanced.LuceneQuery<Question, QuestionIndex>()
						.Highlight(x => x.QuestionText, 100, 1, out highlightings)
						.Search(x => x.QuestionText, badSearchTerm)
						.OrderByScore()
						.FirstOrDefault());
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

		/**
	   * Code taken from http://pastebin.com/a78XzGDk which in-turn is based on
	   * https://gist.github.com/ayende/1669767.
	   * Ref. http://stackoverflow.com/questions/10791164/ravendb-fast-substring-search
	   */
			[NotForQuerying]
		public class NGramAnalyzer : Analyzer
		{
			public override TokenStream TokenStream(string fieldName, TextReader reader)
			{
				var tokenizer = new StandardTokenizer(Version.LUCENE_30, reader) { MaxTokenLength = 255 };
				TokenStream filter = new StandardFilter(tokenizer);
				filter = new LowerCaseFilter(filter);
				filter = new StopFilter(false, filter, StandardAnalyzer.STOP_WORDS_SET);
				return new NGramTokenFilter(filter, 2, 20);
			}
		}

		public class NGramTokenFilter : TokenFilter
		{
			public static int DefaultMinNgramSize = 1;
			public static int DefaultMaxNgramSize = 2;

			private readonly int _maxGram;
			private readonly int _minGram;
			private readonly IOffsetAttribute _offsetAtt;
			private readonly ITermAttribute _termAtt;

			private int _curGramSize;
			private int _curPos;
			private char[] _curTermBuffer;
			private int _curTermLength;
			private int _tokStart;

			/**
			 * Creates NGramTokenFilter with given min and max n-grams.
			 * <param name="input"><see cref="TokenStream"/> holding the input to be tokenized</param>
			 * <param name="minGram">the smallest n-gram to generate</param>
			 * <param name="maxGram">the largest n-gram to generate</param>
			 */
			public NGramTokenFilter(TokenStream input, int minGram, int maxGram)
				: base(input)
			{
				if (minGram < 1)
				{
					throw new ArgumentException("minGram must be greater than zero");
				}
				if (minGram > maxGram)
				{
					throw new ArgumentException("minGram must not be greater than maxGram");
				}
				_minGram = minGram;
				_maxGram = maxGram;

				_termAtt = AddAttribute<ITermAttribute>();
				_offsetAtt = AddAttribute<IOffsetAttribute>();
			}

			/**
			 * Creates NGramTokenFilter with default min and max n-grams.
			 * <param name="input"><see cref="TokenStream"/> holding the input to be tokenized</param>
			 */
			public NGramTokenFilter(TokenStream input)
				: this(input, DefaultMinNgramSize, DefaultMaxNgramSize)
			{
			}

			/** Returns the next token in the stream, or null at EOS. */
			public override bool IncrementToken()
			{
				while (true)
				{
					if (_curTermBuffer == null)
					{
						if (!input.IncrementToken())
						{
							return false;
						}
						_curTermBuffer = (char[])_termAtt.TermBuffer().Clone();
						_curTermLength = _termAtt.TermLength();
						_curGramSize = _minGram;
						_curPos = 0;
						_tokStart = _offsetAtt.StartOffset;
					}
					while (_curGramSize <= _maxGram)
					{
						while (_curPos + _curGramSize <= _curTermLength)
						{
							// while there is input
							ClearAttributes();
							_termAtt.SetTermBuffer(_curTermBuffer, _curPos, _curGramSize);
							_offsetAtt.SetOffset(_tokStart + _curPos, _tokStart + _curPos + _curGramSize);
							_curPos++;
							return true;
						}
						_curGramSize++; // increase n-gram size
						_curPos = 0;
					}
					_curTermBuffer = null;
				}
			}

			public override void Reset()
			{
				base.Reset();
				_curTermBuffer = null;
			}
		}

	}
}
