using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Indexes.Counters
{
    public abstract class AbstractCountersIndexCreationTask : AbstractIndexCreationTaskBase<CountersIndexDefinition>
    {
    }

    public abstract class AbstractCountersIndexCreationTask<TDocument> : AbstractCountersIndexCreationTask<TDocument, TDocument>
    {
    }

    public abstract class AbstractCountersIndexCreationTask<TDocument, TReduceResult> : AbstractGenericCountersIndexCreationTask<TReduceResult>
    {
        private (string Counter, Expression<Func<IEnumerable<CounterEntry>, IEnumerable>> Map) _map;

        /// <summary>
        /// Sets map function for all Counters
        /// </summary>
        protected void AddMapForAll(Expression<Func<IEnumerable<CounterEntry>, IEnumerable>> map)
        {
            AddMapInternal(null, map);
        }

        /// <summary>
        /// Sets map function for specified Counters
        /// </summary>
        protected void AddMap(string counter, Expression<Func<IEnumerable<CounterEntry>, IEnumerable>> map)
        {
            if (typeof(TDocument) == typeof(object))
                throw new NotSupportedException($"Counter name cannot be specified when indexing all documents. Use '{nameof(AddMapForAll)}' method and filter by Counter name in map expression if needed.");

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

        public override CountersIndexDefinition CreateIndexDefinition()
        {
            if (Conventions == null)
                Conventions = new DocumentConventions();

            var builder = new CountersIndexDefinitionBuilder<TDocument, TReduceResult>(IndexName)
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
                State = State,
                DeploymentMode = DeploymentMode
            };

            if (_map != default)
            {
                if (_map.Counter == null)
                    builder.AddMapForAll(_map.Map);
                else
                    builder.AddMap(_map.Counter, _map.Map);
            }

            var indexDefinition = builder.ToIndexDefinition(Conventions);
            return indexDefinition;
        }
    }
}
