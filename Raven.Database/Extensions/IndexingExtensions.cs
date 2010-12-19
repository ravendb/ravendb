using System;
using System.Globalization;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Raven.Database.Data;
using Raven.Database.Indexing;

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

        public static Filter GetFilter(this IndexQuery self)
        {
            var spatialIndexQuery = self as SpatialIndexQuery;
            if(spatialIndexQuery != null)
            {
                var dq = new Lucene.Net.Spatial.Tier.DistanceQueryBuilder(
                    spatialIndexQuery.Latitude,
                    spatialIndexQuery.Longitude,
                    spatialIndexQuery.Radius,
                    SpatialIndex.LatField,
                    SpatialIndex.LngField,
                    Lucene.Net.Spatial.Tier.Projectors.CartesianTierPlotter.DefaltFieldPrefix,
                    true);

                return dq.Filter;
            }
            return null;
        }

        public static Analyzer GetAnalyzer(this IndexDefinition self, string name)
        {
            if (self.Analyzers == null)
                return null;
            string analyzerTypeAsString;
            if (self.Analyzers.TryGetValue(name, out analyzerTypeAsString) == false)
                return null;
            return CreateAnalyzerInstance(name, analyzerTypeAsString);
        }

        public static Field.Index GetIndex(this IndexDefinition self, string name, Field.Index defaultIndex)
        {
            if (self.Indexes == null)
                return defaultIndex;
            FieldIndexing value;
            if (self.Indexes.TryGetValue(name, out value) == false)
            {
                string ignored;
                if (self.Analyzers.TryGetValue(name, out ignored))
                    return Field.Index.ANALYZED;// if there is a custom analyzer, the value should be analyzer
                return defaultIndex;
            }
            switch (value)
            {
                case FieldIndexing.No:
                    return Field.Index.NO;
                case FieldIndexing.NotAnalyzedNoNorms:
                    return Field.Index.NOT_ANALYZED_NO_NORMS;
                case FieldIndexing.Analyzed:
                    return Field.Index.ANALYZED;
                case FieldIndexing.NotAnalyzed:
                    return Field.Index.NOT_ANALYZED;
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
                return defaultStorage;
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

	
        public static Sort GetSort(this IndexQuery self, Filter filter, IndexDefinition indexDefinition)
        {
            var spatialIndexQuery = self as SpatialIndexQuery;
            if(spatialIndexQuery != null && spatialIndexQuery.SortByDistance)
            {
                var dsort = new Lucene.Net.Spatial.Tier.DistanceFieldComparatorSource((Lucene.Net.Spatial.Tier.DistanceFilter)filter);

                return new Sort(new SortField("foo", dsort, false));
		
            }

            if (self.SortedFields != null && self.SortedFields.Length > 0)
                return new Sort(self.SortedFields.Select(x => ToLuceneSortField(indexDefinition, x)).ToArray());
            return null;
        }

        private static SortField ToLuceneSortField(IndexDefinition definition, SortedField sortedField)
        {
            SortOptions? sortOptions = GetSortOption(definition, sortedField.Field);
            if (sortOptions == null)
                return new SortField(sortedField.Field, CultureInfo.InvariantCulture, sortedField.Descending);
            return new SortField(sortedField.Field, (int) sortOptions.Value, sortedField.Descending);
        }

        public static SortOptions? GetSortOption(this IndexDefinition self, string name)
        {
            SortOptions value;
            if (!self.SortOptions.TryGetValue(name, out value))
            {
                if (!name.EndsWith("_Range"))
                {
                    return null;
                }
                string nameWithoutRange = name.Substring(0, name.Length - "_Range".Length);
                if (!self.SortOptions.TryGetValue(nameWithoutRange, out value))
                    return null;
            }
            return value;
        }
    }
}