using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class NGramSearch : RavenTest
	{
		public class Image
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public ICollection<string> Users { get; set; }
			public ICollection<string> Tags { get; set; }
		}

		public class NGramTokenFilter : TokenFilter
		{
			public static int DEFAULT_MIN_NGRAM_SIZE = 1;
			public static int DEFAULT_MAX_NGRAM_SIZE = 2;

			private int minGram, maxGram;

			private char[] curTermBuffer;
			private int curTermLength;
			private int curGramSize;
			private int curPos;
			private int tokStart;

			private TermAttribute termAtt;
			private OffsetAttribute offsetAtt;

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
					throw new System.ArgumentException("minGram must be greater than zero");
				}
				if (minGram > maxGram)
				{
					throw new System.ArgumentException("minGram must not be greater than maxGram");
				}
				this.minGram = minGram;
				this.maxGram = maxGram;

				this.termAtt = (TermAttribute) AddAttribute<ITermAttribute>();
				this.offsetAtt = (OffsetAttribute) AddAttribute<IOffsetAttribute>();
			}

			/**
			 * Creates NGramTokenFilter with default min and max n-grams.
			 * <param name="input"><see cref="TokenStream"/> holding the input to be tokenized</param>
			 */
			public NGramTokenFilter(TokenStream input)
				: this(input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE)
			{

			}

			/** Returns the next token in the stream, or null at EOS. */
			public override bool IncrementToken()
			{
				while (true)
				{
					if (curTermBuffer == null)
					{
						if (!input.IncrementToken())
						{
							return false;
						}
						else
						{
							curTermBuffer = (char[])termAtt.TermBuffer().Clone();
							curTermLength = termAtt.TermLength();
							curGramSize = minGram;
							curPos = 0;
							tokStart = offsetAtt.StartOffset;
						}
					}
					while (curGramSize <= maxGram)
					{
						while (curPos + curGramSize <= curTermLength)
						{     // while there is input
							ClearAttributes();
							termAtt.SetTermBuffer(curTermBuffer, curPos, curGramSize);
							offsetAtt.SetOffset(tokStart + curPos, tokStart + curPos + curGramSize);
							curPos++;
							return true;
						}
						curGramSize++;                         // increase n-gram size
						curPos = 0;
					}
					curTermBuffer = null;
				}
			}

			public override void Reset()
			{
				base.Reset();
				curTermBuffer = null;
			}
		}

		[NotForQuerying]
		public class NGramAnalyzer : Analyzer
		{
			public override TokenStream TokenStream(string fieldName, TextReader reader)
			{
				var tokenizer = new StandardTokenizer(Version.LUCENE_29, reader);
				tokenizer.MaxTokenLength = 255;
				TokenStream filter = new StandardFilter(tokenizer);
				filter = new LowerCaseFilter(filter);
				filter = new StopFilter(false,filter, StandardAnalyzer.STOP_WORDS_SET);
				return new NGramTokenFilter(filter, 2, 6);
			}
		}

		[Fact]
		public void Can_search_inner_words()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new FullTextSearchOnTags.Image { Id = "1", Name = "Great Photo buddy" });
					session.Store(new FullTextSearchOnTags.Image { Id = "2", Name = "Nice Photo of the sky" });
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs.Images select new { doc.Name }",
					Indexes =
				                                        	{
				                                        		{"Name", FieldIndexing.Analyzed}
				                                        	},
					Analyzers =
				                                        	{
				                                        		{"Name", typeof (NGramAnalyzer).AssemblyQualifiedName}
				                                        	}
				});

				using (var session = store.OpenSession())
				{
					var images = session.Query<FullTextSearchOnTags.Image>("test")
						.Customize(x => x.WaitForNonStaleResults())
						.OrderBy(x => x.Name)
						.Search(x => x.Name, "phot")
						.ToList();
					Assert.NotEmpty(images);
				}
			}
		}

	}
}
