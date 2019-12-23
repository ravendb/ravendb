using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Indexes.TimeSeries
{
    /// <summary>
    /// This class provides a way to define a strongly typed index on the client.
    /// </summary>
    public class TimeSeriesIndexDefinitionBuilder<TDocument, TReduceResult> : AbstractIndexDefinitionBuilder<TDocument, TReduceResult, TimeSeriesIndexDefinition>
    {
        public TimeSeriesIndexDefinitionBuilder(string indexName = null)
            : base(indexName)
        {
        }

        private (string TimeSeries, Expression<Func<IEnumerable<AbstractTimeSeriesIndexCreationTask.TimeSeriesSegment>, IEnumerable>> Map) _map;

        public void AddMap(string timeSeries, Expression<Func<IEnumerable<AbstractTimeSeriesIndexCreationTask.TimeSeriesSegment>, IEnumerable>> map)
        {
            if (string.IsNullOrWhiteSpace(timeSeries))
                throw new ArgumentException("TimeSeries name cannot be null or whitespace.", nameof(timeSeries));
            if (map == null)
                throw new ArgumentNullException(nameof(map));

            if (_map != default)
                throw new InvalidOperationException($"You cannot set Map more than once. Use {nameof(AbstractMultiMapTimeSeriesIndexCreationTask)} for this purpose.");

            _map = (timeSeries, map);
        }

        public override TimeSeriesIndexDefinition ToIndexDefinition(DocumentConventions conventions, bool validateMap = true)
        {
            if (_map == default && validateMap)
                throw new InvalidOperationException(string.Format("Map is required to generate an index, you cannot create an index without a valid Map property (in index {0}).", _indexName));

            return base.ToIndexDefinition(conventions, validateMap);
        }

        protected override void ToIndexDefinition(TimeSeriesIndexDefinition indexDefinition, DocumentConventions conventions)
        {
            if (_map == default)
                return;

            var querySource = GetQuerySource(conventions);

            var map = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<AbstractTimeSeriesIndexCreationTask.TimeSeriesSegment, TReduceResult>(
                    _map.Map,
                    conventions,
                    querySource,
                    translateIdentityProperty: true);

            indexDefinition.Maps.Add(map);
        }

        private string GetQuerySource(DocumentConventions conventions)
        {
            var querySource = (typeof(TDocument) == typeof(object))
                ? "timeSeries"
                : IndexDefinitionHelper.GetQuerySource(conventions, typeof(TDocument), IndexSourceType.TimeSeries);

            var timeSeries = _map.TimeSeries;
            if (StringExtensions.IsIdentifier(timeSeries))
                return $"{querySource}.{timeSeries}";

            return $"{querySource}[@\"{timeSeries.Replace("\"", "\"\"")}\"]";
        }
    }

    /// <summary>
    /// This class provides a way to define a strongly typed index on the client.
    /// </summary>
    public class TimeSeriesIndexDefinitionBuilder<TDocument> : TimeSeriesIndexDefinitionBuilder<TDocument, TDocument>
    {
        public TimeSeriesIndexDefinitionBuilder(string indexName = null) : base(indexName)
        {
        }
    }
}
