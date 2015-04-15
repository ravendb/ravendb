using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;
using SpellChecker.Net.Search.Spell;
using Directory = Lucene.Net.Store.Directory;

namespace Raven.Database.Queries
{
	public class SuggestionQueryIndexExtension : IIndexExtension
	{
		private readonly Index _indexInstance;
		private readonly WorkContext workContext;
		private readonly string field;
		private readonly Directory directory;
		private readonly SpellChecker.Net.Search.Spell.SpellChecker spellChecker;
		private string _operationText;

		[CLSCompliant(false)]
		public SuggestionQueryIndexExtension(Index indexInstance, WorkContext workContext, string key, 
			bool isRunInMemory, string field)
		{
			_indexInstance = indexInstance;
			this.workContext = workContext;
			this.field = field;

			if (isRunInMemory)
			{
				directory = new RAMDirectory();
			}
			else
			{
				directory = FSDirectory.Open(new DirectoryInfo(key));
			}

			spellChecker = new SpellChecker.Net.Search.Spell.SpellChecker(directory, null);
			_operationText = "Suggestions for " + field;
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

			var result = new HashSet<string>();
			var maxSuggestions = suggestionQuery.MaxSuggestions;
			foreach (var term in individualTerms)
			{
				if (maxSuggestions <= 0) 
					break;

				foreach (var suggestion in spellChecker.SuggestSimilar(term,
															suggestionQuery.MaxSuggestions, // we can filter out duplicates, so taking more
															indexReader,
															suggestionQuery.Field,
															suggestionQuery.Popularity))
				{
					if (result.Add(suggestion) == false)
						continue;

					maxSuggestions--;
					if (maxSuggestions <= 0)
						break;
				}
			}

			return new SuggestionQueryResult
			{
				Suggestions = result.ToArray()
			};
		}

		public void OnDocumentsIndexed(IEnumerable<Document> documents, Analyzer searchAnalyzer)
		{
			var indexingPerformanceStats = new IndexingPerformanceStats {Operation = _operationText, Started = SystemTime.UtcNow};
			_indexInstance.AddIndexingPerformanceStats(indexingPerformanceStats);
			
			var sp = Stopwatch.StartNew();
			var enumerableDictionary = new EnumerableDictionary(documents, field, searchAnalyzer);
			spellChecker.IndexDictionary(enumerableDictionary, workContext.CancellationToken);
			
			indexingPerformanceStats.Duration = sp.Elapsed;
			indexingPerformanceStats.InputCount = enumerableDictionary.NumberOfDocuments;
			indexingPerformanceStats.ItemsCount = enumerableDictionary.NumberOfTokens;
			indexingPerformanceStats.OutputCount = enumerableDictionary.NumberOfFields;
		}

		public string Name { get { return "Suggestions"; } }

		public class EnumerableDictionary : SpellChecker.Net.Search.Spell.IDictionary
		{
			private readonly IEnumerable<Document> documents;
			private readonly string field;
			private readonly Analyzer searchAnalyzer;

			public int NumberOfDocuments;
			public int NumberOfFields;
			public int NumberOfTokens;
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
					NumberOfDocuments++;
					if (document == null)
						continue;
					var fieldables = document.GetFieldables(field);
					if (fieldables == null)
						continue;
					foreach (var fieldable in fieldables)
					{
						NumberOfFields++;
						if (fieldable == null)
							continue;
						var str = fieldable.StringValue;
						if (string.IsNullOrEmpty(str))
							continue;
						var tokenStream = searchAnalyzer.ReusableTokenStream(field, new StringReader(str));
						while (tokenStream.IncrementToken())
						{
							NumberOfTokens++;
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
