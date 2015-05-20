//-----------------------------------------------------------------------
// <copyright file="IndexingExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Linq;
using System.Reflection;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Database.Indexing.Sorting;
using Raven.Database.Indexing.Sorting.AlphaNumeric;
using Raven.Database.Indexing.Sorting.Custom;
using Raven.Database.Linq;
using Raven.Database.Util;
using Spatial4n.Core.Shapes;
using Spatial4n.Core.Shapes.Impl;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Database.Extensions
{
	public static class IndexingExtensions
	{
		public static Analyzer CreateAnalyzerInstance(string name, string analyzerTypeAsString)
		{
			var analyzerType = GetAnalyzerType(name, analyzerTypeAsString);

			try
			{
				var assembly = analyzerType.Assembly;

				// try to get parameterless ctor
				var ctors = analyzerType.GetConstructor(Type.EmptyTypes);
				if (ctors != null)
					return (Analyzer)Activator.CreateInstance(assembly.FullName, analyzerType.FullName).Unwrap();

                ctors = analyzerType.GetConstructor(new[] { typeof(Lucene.Net.Util.Version) });
				if (ctors != null) 
					return (Analyzer)Activator
						.CreateInstance(assembly.FullName, analyzerType.FullName, false, BindingFlags.CreateInstance | BindingFlags.Public | BindingFlags.Instance, null, new object[] { Lucene.Net.Util.Version.LUCENE_30 }, CultureInfo.InvariantCulture, null)
						.Unwrap();
			}
			catch (Exception e)
			{
				throw new InvalidOperationException(
					"Could not create new analyzer instance '" + analyzerTypeAsString + "' for field: " +
						name, e);
			}

			throw new InvalidOperationException(
				"Could not create new analyzer instance '" + analyzerTypeAsString + "' for field: " + name + ". No recognizable constructor found.");
		}

		public static Type GetAnalyzerType(string name, string analyzerTypeAsString)
		{
			var luceneAssembly = typeof (StandardAnalyzer).Assembly;
			var analyzerType = luceneAssembly.GetType(analyzerTypeAsString) ??
			                   Type.GetType(analyzerTypeAsString) ??
			                   luceneAssembly.GetType("Lucene.Net.Analysis." + analyzerTypeAsString) ??
			                   luceneAssembly.GetType("Lucene.Net.Analysis.Standard." + analyzerTypeAsString);
			if (analyzerType == null)
				throw new InvalidOperationException("Cannot find analyzer type '" + analyzerTypeAsString + "' for field: " + name);
			return analyzerType;
		}

		public static Field.Index GetIndex(this IndexDefinition self, string name, Field.Index? defaultIndex)
		{
			if (self.Indexes == null)
				return defaultIndex ?? Field.Index.ANALYZED_NO_NORMS;
			FieldIndexing value;
			if (self.Indexes.TryGetValue(name, out value) == false)
			{
				if (self.Indexes.TryGetValue(Constants.AllFields, out value) == false)
				{
					string ignored;
					if (self.Analyzers.TryGetValue(name, out ignored) ||
						self.Analyzers.TryGetValue(Constants.AllFields, out ignored))
					{
						return Field.Index.ANALYZED; // if there is a custom analyzer, the value should be analyzed
					}
					return defaultIndex ?? Field.Index.ANALYZED_NO_NORMS;
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
					return defaultIndex ?? Field.Index.ANALYZED_NO_NORMS;
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
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		public static Field.TermVector GetTermVector(this IndexDefinition self, string name, Field.TermVector defaultTermVector)
		{
			if (self.TermVectors == null)
				return defaultTermVector;

			FieldTermVector value;
			if (self.TermVectors.TryGetValue(name, out value) == false)
				return defaultTermVector;

			if (value != FieldTermVector.No && self.GetIndex(name, null) == Field.Index.NO)
			{
				throw new InvalidOperationException(string.Format("TermVectors cannot be enabled for the field {0} because Indexing is set to No", name));
			}

			switch (value)
			{
				case FieldTermVector.No:
					return Field.TermVector.NO;
				case FieldTermVector.WithOffsets:
					return Field.TermVector.WITH_OFFSETS;
				case FieldTermVector.WithPositions:
					return Field.TermVector.WITH_POSITIONS;
				case FieldTermVector.WithPositionsAndOffsets:
					return Field.TermVector.WITH_POSITIONS_OFFSETS;
				case FieldTermVector.Yes:
					return Field.TermVector.YES;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		[CLSCompliant(false)]
		public static Sort GetSort(this IndexQuery self, IndexDefinition indexDefinition, AbstractViewGenerator viewGenerator)
		{
			var spatialQuery = self as SpatialIndexQuery;
			var sortedFields = self.SortedFields;
			if (sortedFields == null || sortedFields.Length <= 0)
			{
				if (spatialQuery == null || string.IsNullOrEmpty(self.Query) == false)
					return null;
				sortedFields = new[] { new SortedField(Constants.DistanceFieldName), };
			}

			return new Sort(sortedFields
							.Select(sortedField =>
							{
								if (sortedField.Field == Constants.TemporaryScoreValue)
								{
									return SortField.FIELD_SCORE;
								}
								if (sortedField.Field.StartsWith(Constants.AlphaNumericFieldName))
								{
									var customField = SortFieldHelper.CustomField(sortedField.Field);
									if (string.IsNullOrEmpty(customField.Name))
										throw new InvalidOperationException("Alphanumeric sort: cannot figure out what field to sort on!");

									var anSort = new AlphaNumericComparatorSource();
									return new SortField(customField.Name, anSort, sortedField.Descending);
								}
								if (sortedField.Field.StartsWith(Constants.RandomFieldName))
								{
									var customField = SortFieldHelper.CustomField(sortedField.Field);
									
									if (string.IsNullOrEmpty(customField.Name)) // truly random
										return new RandomSortField(Guid.NewGuid().ToString());

									return new RandomSortField(customField.Name);
								}
								if (sortedField.Field.StartsWith(Constants.CustomSortFieldName))
								{
									var customField = SortFieldHelper.CustomField(sortedField.Field);
									if (string.IsNullOrEmpty(customField.Name))
										throw new InvalidOperationException("Custom sort: cannot figure out what field to sort on!");

									//backward compatibility for old clients
									var isDescending = sortedField.Descending || customField.Type[customField.Type.Length - 1] == '-';
									return new CustomSortField(customField.Name, self, isDescending);
								}
								if (sortedField.Field.StartsWith(Constants.DistanceFieldName))
								{
								    SpatialField spatialField = null;
								    Shape shape ;

                                    if (sortedField.Field.Length == Constants.DistanceFieldName.Length) 
                                    {
                                        if (spatialQuery == null)
                                            throw new InvalidOperationException("Illegal Spatial Sort Parameter: Blank Spatial sort cannot be used without a spatial query");

                                        spatialField = viewGenerator.GetSpatialField(spatialQuery.SpatialFieldName);

                                        shape = spatialField.ReadShape(spatialQuery.QueryShape);
                                    }
                                    else
                                    {
                                        var sortParams = sortedField.Field.Split(';');
                                        double lat, lng;

                                        if (sortParams.Length <3 || !double.TryParse(sortParams[1], out lat) || !double.TryParse(sortParams[2], out lng))
                                            throw new InvalidOperationException("Illegal Spatial Sort Parameter");


                                        string spatialFieldName;

                                        if (sortParams.Length >= 4)
                                        {
                                            spatialFieldName = sortParams[3];
                                        }
                                        else
                                        {
                                            if (spatialQuery == null)
                                            {
                                                spatialFieldName = Constants.DefaultSpatialFieldName;
                                            }
                                            else
                                            {
                                                spatialFieldName  = spatialQuery.SpatialFieldName;
                                            }
                                        }

                                        spatialField = viewGenerator.GetSpatialField(spatialFieldName);

                                        shape = new PointImpl(lng, lat, spatialField.GetContext());    
                                    }
									var dsort = new SpatialDistanceFieldComparatorSource(spatialField, shape.GetCenter());
                                    return new SortField(sortedField.Field, dsort, sortedField.Descending);
								}

								var sortOptions = GetSortOption(indexDefinition, sortedField.Field, self);

								if (sortOptions == null || sortOptions == SortOptions.None)
									return new SortField(sortedField.Field, CultureInfo.InvariantCulture, sortedField.Descending);

                                if (sortOptions.Value == SortOptions.Short)
							        sortOptions = SortOptions.Int;
							    return new SortField(sortedField.Field, (int)sortOptions.Value, sortedField.Descending);

							})
							.ToArray());
		}

		private const string _Range = "_Range";
        private const string _SortHint = "SortHint-";
        private static CompareInfo InvariantCompare = CultureInfo.InvariantCulture.CompareInfo;

		public static SortOptions? GetSortOption(this IndexDefinition self, string name, IndexQuery query)
		{
            SortOptions value;

            if (InvariantCompare.IsSuffix(name, _Range, CompareOptions.None))
			{
                string nameWithoutRange = name.Substring(0, name.Length - _Range.Length);
				if (self.SortOptions.TryGetValue(nameWithoutRange, out value))
					return value;

				if (self.SortOptions.TryGetValue(Constants.AllFields, out value))
					return value;

                if (query != null && query.SortHints != null && query.SortHints.TryGetValue(_SortHint + nameWithoutRange, out value))
                    return value;
			}

			if (self.SortOptions.TryGetValue(name, out value))
				return value;

			if (self.SortOptions.TryGetValue(Constants.AllFields, out value))
				return value;

			if (query == null || query.SortHints == null)
				return value;

            if (!query.SortHints.TryGetValue(_SortHint + name, out value))
                return SortOptions.None;

            return value;
		}
	}
}