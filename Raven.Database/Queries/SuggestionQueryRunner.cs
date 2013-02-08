//-----------------------------------------------------------------------
// <copyright file="SuggestionQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using SpellChecker.Net.Search.Spell;
using Task = System.Threading.Tasks.Task;

namespace Raven.Database.Queries
{
	public class SuggestionQueryRunner
	{
		private readonly DocumentDatabase database;

		public SuggestionQueryRunner(DocumentDatabase database)
		{
			this.database = database;
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
													  database.Configuration.MaxPageSize);

			var indexExtensionKey = MonoHttpUtility.UrlEncode(suggestionQuery.Field + "-" + suggestionQuery.Distance + "-" + suggestionQuery.Accuracy);

			var indexExtension = (
				                     database.IndexStorage.GetIndexExtension(indexName, indexExtensionKey) ??
									 database.IndexStorage.GetIndexExtensionByPrefix(indexName, MonoHttpUtility.UrlEncode(suggestionQuery.Field +"-"+suggestionQuery.Distance)) ??
									 database.IndexStorage.GetIndexExtensionByPrefix(indexName, MonoHttpUtility.UrlEncode(suggestionQuery.Field)) 
			                     ) as SuggestionQueryIndexExtension;


			IndexSearcher currentSearcher;
			using (database.IndexStorage.GetCurrentIndexSearcher(indexName, out currentSearcher))
			{
				if (currentSearcher == null)
				{
					throw new InvalidOperationException("Could not find current searcher");
				}
				var indexReader = currentSearcher.IndexReader;

				if (indexExtension != null)
					return indexExtension.Query(suggestionQuery, indexReader);


				var suggestionQueryIndexExtension = new SuggestionQueryIndexExtension(
					database.WorkContext,
					Path.Combine(database.Configuration.IndexStoragePath, "Raven-Suggestions", indexName, indexExtensionKey),
					indexReader.Directory() is RAMDirectory,
					GetStringDistance(suggestionQuery.Distance),
					suggestionQuery.Field,
					suggestionQuery.Accuracy);

				database.IndexStorage.SetIndexExtension(indexName, indexExtensionKey, suggestionQueryIndexExtension);

				long _;
				var task = Task.Factory.StartNew(() => suggestionQueryIndexExtension.Init(indexReader));
				database.AddTask(task, new RavenJObject(), out _);

				// wait for a bit for the suggestions to complete, but not too much (avoid IIS resets)
				task.Wait(15000, database.WorkContext.CancellationToken);

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
