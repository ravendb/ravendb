//-----------------------------------------------------------------------
// <copyright file="SuggestionQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Web;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using SpellChecker.Net.Search.Spell;

namespace Raven.Database.Queries
{
	public class SuggestionQueryRunner
	{
		private readonly DocumentDatabase _database;

		public SuggestionQueryRunner(DocumentDatabase database)
		{
			_database = database;
		}

		public SuggestionQueryResult ExecuteSuggestionQuery(string indexName, SuggestionQuery suggestionQuery)
		{
			if (suggestionQuery == null) throw new ArgumentNullException("suggestionQuery");
			if (string.IsNullOrWhiteSpace(suggestionQuery.Term)) throw new ArgumentNullException("suggestionQuery.Term");
			if (string.IsNullOrWhiteSpace(indexName)) throw new ArgumentNullException("indexName");
			if (string.IsNullOrWhiteSpace(suggestionQuery.Field)) throw new ArgumentNullException("suggestionQuery.Field");
			if (suggestionQuery.MaxSuggestions <= 0) suggestionQuery.MaxSuggestions = 10;
			if (suggestionQuery.Accuracy <= 0 || suggestionQuery.Accuracy > 1) suggestionQuery.Accuracy = 0.5f;

			suggestionQuery.MaxSuggestions = Math.Min(suggestionQuery.MaxSuggestions,
													  _database.Configuration.MaxPageSize);

			var indexExtensionKey = MonoHttpUtility.UrlEncode(suggestionQuery.Field + "-" + suggestionQuery.Distance + "-" + suggestionQuery.Accuracy);

			var indexExtension = _database.IndexStorage.GetIndexExtension(indexName, indexExtensionKey) as SuggestionQueryIndexExtension;


			IndexSearcher currentSearcher;
			using(_database.IndexStorage.GetCurrentIndexSearcher(indexName,out currentSearcher))
			{
				var indexReader = currentSearcher.IndexReader;

				if (indexExtension != null)
					return indexExtension.Query(suggestionQuery, indexReader);


				var suggestionQueryIndexExtension = new SuggestionQueryIndexExtension(
					Path.Combine(_database.Configuration.IndexStoragePath, "Raven-Suggestions", indexName, indexExtensionKey),
					indexReader,
					GetStringDistance(suggestionQuery.Distance), 
					suggestionQuery.Field, 
					suggestionQuery.Accuracy);
				suggestionQueryIndexExtension.Init(indexReader);

				_database.IndexStorage.SetIndexExtension(indexName, indexExtensionKey, suggestionQueryIndexExtension);

				return suggestionQueryIndexExtension.Query(suggestionQuery, indexReader);
			}
		}

		[CLSCompliant(false)]
		public static StringDistance GetStringDistance(StringDistanceTypes distanceAlg)
		{
			switch (distanceAlg)
			{
				case StringDistanceTypes.NGram:
					return new NGramDistance();
				case StringDistanceTypes.JaroWinkler:
					return new JaroWinklerDistance();
				default:
					return new LevenshteinDistance();
			}
		}
	}
}
