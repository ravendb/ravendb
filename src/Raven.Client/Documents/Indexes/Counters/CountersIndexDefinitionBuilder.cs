using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Indexes.Counters
{
    /// <summary>
    /// This class provides a way to define a strongly typed index on the client.
    /// </summary>
    public class CountersIndexDefinitionBuilder<TDocument, TReduceResult> : AbstractIndexDefinitionBuilder<TDocument, TReduceResult, CountersIndexDefinition>
    {
        public CountersIndexDefinitionBuilder(string indexName = null)
            : base(indexName)
        {
        }

        private (string Counter, Expression<Func<IEnumerable<CounterEntry>, IEnumerable>> Map) _map;

        /// <summary>
        /// Sets map function for all TimeSeries
        /// </summary>
        public void AddMapForAll(Expression<Func<IEnumerable<CounterEntry>, IEnumerable>> map)
        {
            AddMapInternal(null, map);
        }

        /// <summary>
        /// Sets map function for specified TimeSeries
        /// </summary>
        public void AddMap(string counter, Expression<Func<IEnumerable<CounterEntry>, IEnumerable>> map)
        {
            if (string.IsNullOrWhiteSpace(counter))
                throw new ArgumentException("Counter name cannot be null or whitespace.", nameof(counter));

            AddMapInternal(counter, map);
        }

        private void AddMapInternal(string counter, Expression<Func<IEnumerable<CounterEntry>, IEnumerable>> map)
        {
            if (map == null)
                throw new ArgumentNullException(nameof(map));

            if (_map != default)
                throw new InvalidOperationException($"You cannot set Map more than once. Use {nameof(AbstractMultiMapCountersIndexCreationTask)} for this purpose.");

            _map = (counter, map);
        }

        public override CountersIndexDefinition ToIndexDefinition(DocumentConventions conventions, bool validateMap = true)
        {
            if (_map == default && validateMap)
                throw new InvalidOperationException(string.Format("Map is required to generate an index, you cannot create an index without a valid Map property (in index {0}).", _indexName));

            return base.ToIndexDefinition(conventions, validateMap);
        }

        protected override void ToIndexDefinition(CountersIndexDefinition indexDefinition, DocumentConventions conventions)
        {
            if (_map == default)
                return;

            var querySource = GetQuerySource(conventions);

            var map = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<CounterEntry, TReduceResult>(
                    _map.Map,
                    conventions,
                    querySource,
                    translateIdentityProperty: true);

            indexDefinition.Maps.Add(map);
        }

        private string GetQuerySource(DocumentConventions conventions)
        {
            var querySource = (typeof(TDocument) == typeof(object))
                ? "counters"
                : IndexDefinitionHelper.GetQuerySource(conventions, typeof(TDocument), IndexSourceType.Counters);

            var counters = _map.Counter;
            if (counters == null)
                return querySource;

            if (StringExtensions.IsIdentifier(counters))
                return $"{querySource}.{counters}";

            return $"{querySource}[@\"{counters.Replace("\"", "\"\"")}\"]";
        }
    }

    /// <summary>
    /// This class provides a way to define a strongly typed index on the client.
    /// </summary>
    public class CountersIndexDefinitionBuilder<TDocument> : CountersIndexDefinitionBuilder<TDocument, TDocument>
    {
        public CountersIndexDefinitionBuilder(string indexName = null) : base(indexName)
        {
        }
    }
}
