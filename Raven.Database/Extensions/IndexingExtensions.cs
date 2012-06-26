//-----------------------------------------------------------------------
// <copyright file="IndexingExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing.Sorting;
using Raven.Database.Server;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Database.Extensions
{
	public static class IndexingExtensions
	{
		public static Analyzer CreateAnalyzerInstance(string name, string analyzerTypeAsString)
		{
			var analyzerType = typeof(StandardAnalyzer).Assembly.GetType(analyzerTypeAsString) ??
				Type.GetType(analyzerTypeAsString);
			if (analyzerType == null)
				throw new InvalidOperationException("Cannot find analzyer type '" + analyzerTypeAsString + "' for field: " + name);
			try
			{
				return (Analyzer)Activator.CreateInstance(analyzerType);
			}
			catch (Exception e)
			{
				throw new InvalidOperationException(
					"Could not create new analyzer instance '" + name + "' for field: " +
						name, e);
			}
		}

		public static Field.Index GetIndex(this IndexDefinition self, string name, Field.Index defaultIndex)
		{
			if (self.Indexes == null)
				return defaultIndex;
			FieldIndexing value;
			if (self.Indexes.TryGetValue(name, out value) == false)
			{
				if(self.Indexes.TryGetValue(Constants.AllFields, out value) == false)
				{
					string ignored;
					if (self.Analyzers.TryGetValue(name, out ignored) ||
						self.Analyzers.TryGetValue(Constants.AllFields, out ignored))
					{
						return Field.Index.ANALYZED; // if there is a custom analyzer, the value should be analyzed
					}
					return defaultIndex;
				}
			}
			switch (value)
			{
				case FieldIndexing.No:
					return Field.Index.NO;
				case FieldIndexing.Analyzed:
					return Field.Index.ANALYZED_NO_NORMS;
				case FieldIndexing.NotAnalyzed:
					return Field.Index.NOT_ANALYZED_NO_NORMS;
				case FieldIndexing.Default:
					return defaultIndex;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		public static Field.Store GetStorage(this IndexDefinition self, string name, Field.Store defaultStorage)
		{
			if (self.Stores == null)
				return defaultStorage;
			FieldStorage value;
			if (self.Stores.TryGetValue(name, out value) == false)
			{
				// do we have a overriding default?
				if (self.Stores.TryGetValue(Constants.AllFields, out value) == false)
					return defaultStorage;
			}
			switch (value)
			{
				case FieldStorage.Yes:
					return Field.Store.YES;
				case FieldStorage.No:
					return Field.Store.NO;
				case FieldStorage.Compress:
					return Field.Store.COMPRESS;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		public static Sort GetSort(this IndexQuery self, IndexDefinition indexDefinition)
		{
			if (self.SortedFields == null || self.SortedFields.Length <= 0)
				return null;

			var spatialQuery = self as SpatialIndexQuery;

			return new Sort(self.SortedFields
							.Select(sortedField =>
							{
								if(sortedField.Field.StartsWith(Constants.RandomFieldName))
								{
									var parts = sortedField.Field.Split(new[]{';'}, StringSplitOptions.RemoveEmptyEntries);
									if (parts.Length < 2) // truly random
										return new RandomSortField(Guid.NewGuid().ToString());
									return new RandomSortField(parts[1]);
								}
								if (spatialQuery != null && sortedField.Field == Constants.DistanceFieldName)
								{
									var dsort = new SpatialDistanceFieldComparatorSource(spatialQuery.Latitude, spatialQuery.Longitude);
									return new SortField(Constants.DistanceFieldName, dsort, sortedField.Descending);
								}
								var sortOptions = GetSortOption(indexDefinition, sortedField.Field);
								if (sortOptions == null || sortOptions == SortOptions.None)
									return new SortField(sortedField.Field, CultureInfo.InvariantCulture, sortedField.Descending);
								return new SortField(sortedField.Field, (int)sortOptions.Value, sortedField.Descending);
							})
							.ToArray());
		}

		public static SortOptions? GetSortOption(this IndexDefinition self, string name)
		{
			SortOptions value;
			if (self.SortOptions.TryGetValue(name, out value))
			{
				return value;
			}
			if (self.SortOptions.TryGetValue(Constants.AllFields, out value))
				return value;

			if (name.EndsWith("_Range"))
			{
				string nameWithoutRange = name.Substring(0, name.Length - "_Range".Length);
				if (self.SortOptions.TryGetValue(nameWithoutRange, out value))
					return value;

				if (self.SortOptions.TryGetValue(Constants.AllFields, out value))
					return value;
			}
			if (CurrentOperationContext.Headers.Value == null)
				return value;

			var hint = CurrentOperationContext.Headers.Value["SortHint-" + name];
			if (string.IsNullOrEmpty(hint))
				return value;
			Enum.TryParse(hint, true, out value);
			return value;
		}
	}
}