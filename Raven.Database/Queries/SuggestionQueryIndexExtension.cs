using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Abstractions.Data;
using Raven.Database.Indexing;
using SpellChecker.Net.Search.Spell;

namespace Raven.Database.Queries
{
	public class SuggestionQueryIndexExtension : IIndexExtension
	{
		private readonly string field;
		private readonly Directory directory = new RAMDirectory();
		private readonly SpellChecker.Net.Search.Spell.SpellChecker spellChecker;

		public SuggestionQueryIndexExtension(StringDistance distance, string field)
		{
			this.field = field;
			this.spellChecker = new SpellChecker.Net.Search.Spell.SpellChecker(directory, distance);
		}

		public void Init(IndexReader reader)
		{
			spellChecker.IndexDictionary(new LuceneDictionary(reader, field));
		}

		public SuggestionQueryResult Query(SuggestionQuery suggestionQuery)
		{
			spellChecker.SetAccuracy(suggestionQuery.Accuracy);

			var suggestions = spellChecker.SuggestSimilar(suggestionQuery.Term,
			                                              suggestionQuery.MaxSuggestions,
			                                              null,
			                                              suggestionQuery.Field,
			                                              true);

			return new SuggestionQueryResult
			{
				Suggestions = suggestions
			};
		}

		public void OnDocumentsIndexed(IEnumerable<Document> documents)
		{
			spellChecker.IndexDictionary(new EnumerableDictionary(documents, field));
		}

		public class EnumerableDictionary : Dictionary
		{
			private readonly IEnumerable<Document> documents;
			private readonly string field;

			public EnumerableDictionary(IEnumerable<Document> documents, string field)
			{
				this.documents = documents;
				this.field = field;
			}

			public IEnumerator GetWordsIterator()
			{
				return (from document in documents 
						from fieldable in document.GetFieldables(field) 
						select fieldable.StringValue()
						).GetEnumerator();
			}
		}

		public void Dispose()
		{
			spellChecker.Close();
			GC.SuppressFinalize(spellChecker);//stupid! but it doens't do it on its own
		}
	}
}