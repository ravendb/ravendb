using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Indexes.TimeSeries
{
    public abstract class AbstractTimeSeriesIndexCreationTask : AbstractIndexCreationTaskBase<TimeSeriesIndexDefinition>
    {
        public class TimeSeriesSegment
        {
            public string DocumentId { get; set; }

            public TimeSeriesSegmentEntry[] Entries { get; set; }
        }

        public class TimeSeriesSegmentEntry
        {
            public string Tag { get; set; }

            public DateTime TimeStamp { get; set; }

            public double[] Values { get; set; }

            public double Value => Values[0];
        }
    }

    public class AbstractTimeSeriesIndexCreationTask<TDocument> : AbstractTimeSeriesIndexCreationTask<TDocument, TDocument>
    {
    }

    public class AbstractTimeSeriesIndexCreationTask<TDocument, TReduceResult> : AbstractGenericTimeSeriesIndexCreationTask<TReduceResult>
    {
        private (string TimeSeries, Expression<Func<IEnumerable<TimeSeriesSegment>, IEnumerable>> Map) _map;

        protected void AddMap(string timeSeries, Expression<Func<IEnumerable<TimeSeriesSegment>, IEnumerable>> map)
        {
            if (string.IsNullOrWhiteSpace(timeSeries))
                throw new ArgumentException("TimeSeries name cannot be null or whitespace.", nameof(timeSeries));
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
                Configuration = Configuration
            };

            if (_map != default)
                builder.AddMap(_map.TimeSeries, _map.Map);

            var indexDefinition = builder.ToIndexDefinition(Conventions);
            return indexDefinition;
        }
    }
}
