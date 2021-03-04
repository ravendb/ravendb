using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Indexes.TimeSeries
{
    public abstract class AbstractTimeSeriesIndexCreationTask : AbstractIndexCreationTaskBase<TimeSeriesIndexDefinition>
    {
    }

    public abstract class AbstractTimeSeriesIndexCreationTask<TDocument> : AbstractTimeSeriesIndexCreationTask<TDocument, TDocument>
    {
    }

    public class AbstractTimeSeriesIndexCreationTask<TDocument, TReduceResult> : AbstractGenericTimeSeriesIndexCreationTask<TReduceResult>
    {
        private (string TimeSeries, Expression<Func<IEnumerable<TimeSeriesSegment>, IEnumerable>> Map) _map;

        /// <summary>
        /// Sets map function for all TimeSeries
        /// </summary>
        protected void AddMapForAll(Expression<Func<IEnumerable<TimeSeriesSegment>, IEnumerable>> map)
        {
            AddMapInternal(null, map);
        }

        /// <summary>
        /// Sets map function for specified TimeSeries
        /// </summary>
        protected void AddMap(string timeSeries, Expression<Func<IEnumerable<TimeSeriesSegment>, IEnumerable>> map)
        {
            if (typeof(TDocument) == typeof(object))
                throw new NotSupportedException($"TimeSeries name cannot be specified when indexing all documents. Use '{nameof(AddMapForAll)}' method and filter by TimeSeries name in map expression if needed.");

            if (string.IsNullOrWhiteSpace(timeSeries))
                throw new ArgumentException("TimeSeries name cannot be null or whitespace.", nameof(timeSeries));

            AddMapInternal(timeSeries, map);
        }

        private void AddMapInternal(string timeSeries, Expression<Func<IEnumerable<TimeSeriesSegment>, IEnumerable>> map)
        {
            if (map == null)
                throw new ArgumentNullException(nameof(map));

            if (_map != default)
                throw new InvalidOperationException($"You cannot set Map more than once. Use {nameof(AbstractMultiMapTimeSeriesIndexCreationTask)} for this purpose.");

            _map = (timeSeries, map);
        }

        public override TimeSeriesIndexDefinition CreateIndexDefinition()
        {
            if (Conventions == null)
                Conventions = new DocumentConventions();

            var builder = new TimeSeriesIndexDefinitionBuilder<TDocument, TReduceResult>(IndexName)
            {
                Indexes = Indexes,
                IndexesStrings = IndexesStrings,
                Analyzers = Analyzers,
                AnalyzersStrings = AnalyzersStrings,
                Reduce = Reduce,
                Stores = Stores,
                StoresStrings = StoresStrings,
                SuggestionsOptions = IndexSuggestions,
                TermVectors = TermVectors,
                TermVectorsStrings = TermVectorsStrings,
                SpatialIndexes = SpatialIndexes,
                SpatialIndexesStrings = SpatialIndexesStrings,
                OutputReduceToCollection = OutputReduceToCollection,
                PatternForOutputReduceToCollectionReferences = PatternForOutputReduceToCollectionReferences,
                AdditionalSources = AdditionalSources,
                AdditionalAssemblies = AdditionalAssemblies,
                Configuration = Configuration,
                LockMode = LockMode,
                Priority = Priority,
                State = State
            };

            if (_map != default)
            {
                if (_map.TimeSeries == null)
                    builder.AddMapForAll(_map.Map);
                else
                    builder.AddMap(_map.TimeSeries, _map.Map);
            }

            var indexDefinition = builder.ToIndexDefinition(Conventions);
            return indexDefinition;
        }
    }
}
