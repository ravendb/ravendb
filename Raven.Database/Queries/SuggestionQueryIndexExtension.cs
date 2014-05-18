using System;
using System.Collections.Generic;
using System.IO;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;
using SpellChecker.Net.Search.Spell;
using Directory = Lucene.Net.Store.Directory;

namespace Raven.Database.Queries
{
	public class SuggestionQueryIndexExtension : IIndexExtension
	{
		private readonly WorkContext workContext;
		private readonly string key;
		private readonly string field;
		private readonly Directory directory;
		private readonly SpellChecker.Net.Search.Spell.SpellChecker spellChecker;

		[CLSCompliant(false)]
		public SuggestionQueryIndexExtension(
			WorkContext workContext,
			string key,
			StringDistance distanceType,
			bool isRunInMemory,
			string field,
			float accuracy)
		{
			this.workContext = workContext;
			this.key = key;
			this.field = field;

			if (isRunInMemory)
			{
				directory = new RAMDirectory();
			}
			else
			{
				directory = FSDirectory.Open(new DirectoryInfo(key));
			}

			this.spellChecker = new SpellChecker.Net.Search.Spell.SpellChecker(directory, null);
			this.spellChecker.SetAccuracy(accuracy);
			this.spellChecker.setStringDistance(distanceType);
		}

		public void Init(IndexReader reader)
		{
			spellChecker.IndexDictionary(new LuceneDictionary(reader, field), workContext.CancellationToken);
		}

		public SuggestionQueryResult Query(SuggestionQuery suggestionQuery, IndexReader indexReader)
		{
			if (suggestionQuery.Accuracy.HasValue == false)
				throw new InvalidOperationException("SuggestionQuery.Accuracy must be specified.");

			if (suggestionQuery.Distance.HasValue == false)
				throw new InvalidOperationException("SuggestionQuery.Distance must be specified.");

			spellChecker.setStringDistance(SuggestionQueryRunner.GetStringDistance(suggestionQuery.Distance.Value));
			spellChecker.SetAccuracy(suggestionQuery.Accuracy.Value);

			if (suggestionQuery.Term.StartsWith("<<") && suggestionQuery.Term.EndsWith(">>"))
			{
				return QueryOverMultipleWords(suggestionQuery, indexReader,
					suggestionQuery.Term.Substring(2, suggestionQuery.Term.Length - 4));
			}
			if (suggestionQuery.Term.StartsWith("(") && suggestionQuery.Term.EndsWith(")"))
			{
				return QueryOverMultipleWords(suggestionQuery, indexReader,
					suggestionQuery.Term.Substring(1, suggestionQuery.Term.Length - 2));
			}
			string[] suggestions = spellChecker.SuggestSimilar(suggestionQuery.Term,
															   suggestionQuery.MaxSuggestions,
															   indexReader,
															   suggestionQuery.Field,
															   true);

			return new SuggestionQueryResult
			{
				Suggestions = suggestions
			};
		}

		private SuggestionQueryResult QueryOverMultipleWords(SuggestionQuery suggestionQuery, IndexReader indexReader,
															 string queryText)
		{
			var individualTerms = queryText.Split(new[] { ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
			var result = new List<string>();

			foreach (var term in individualTerms)
			{
				result.AddRange(spellChecker.SuggestSimilar(term,
															suggestionQuery.MaxSuggestions,
															indexReader,
															suggestionQuery.Field,
															suggestionQuery.Popularity));
			}

			return new SuggestionQueryResult
			{
				Suggestions = result.ToArray()
			};
		}

		public void OnDocumentsIndexed(IEnumerable<Document> documents, Analyzer searchAnalyzer)
		{
			spellChecker.IndexDictionary(new EnumerableDictionary(documents, field, searchAnalyzer), workContext.CancellationToken);
		}

		public class EnumerableDictionary : SpellChecker.Net.Search.Spell.IDictionary
		{
			private readonly IEnumerable<Document> documents;
			private readonly string field;
			private readonly Analyzer searchAnalyzer;

			public EnumerableDictionary(IEnumerable<Document> documents, string field, Analyzer searchAnalyzer)
			{
				this.documents = documents;
				this.field = field;
				this.searchAnalyzer = searchAnalyzer;
			}

			public IEnumerator<string> GetWordsIterator()
			{
				foreach (var document in documents)
				{
					if (document == null)
						continue;
					var fieldables = document.GetFieldables(field);
					if (fieldables == null)
						continue;
					foreach (var fieldable in fieldables)
					{
						if (fieldable == null)
							continue;
						var str = fieldable.StringValue;
						if (string.IsNullOrEmpty(str))
							continue;
						var tokenStream = searchAnalyzer.ReusableTokenStream(field, new StringReader(str));
						while (tokenStream.IncrementToken())
						{
							var term = tokenStream.GetAttribute<ITermAttribute>();
							yield return term.Term;
						}
					}
				}
			}
		}

		public void Dispose()
		{
			spellChecker.Close();
			GC.SuppressFinalize(spellChecker);//stupid! but it doesn't do it on its own
		}
	}
}
