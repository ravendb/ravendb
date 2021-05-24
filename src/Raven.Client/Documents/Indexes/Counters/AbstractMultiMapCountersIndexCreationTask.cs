using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Indexes.Counters
{
    /// <summary>
    /// Allow to create indexes with multiple maps
    /// </summary>
    public abstract class AbstractMultiMapCountersIndexCreationTask<TReduceResult> : AbstractGenericCountersIndexCreationTask<TReduceResult>
    {
        private readonly List<Func<string>> _maps = new List<Func<string>>();

        protected void AddMap<TSource>(string counter, Expression<Func<IEnumerable<CounterEntry>, IEnumerable>> map)
        {
            if (string.IsNullOrWhiteSpace(counter))
                throw new ArgumentException("Counter name cannot be null or whitespace.", nameof(counter));
            if (map == null)
                throw new ArgumentNullException(nameof(map));

            _maps.Add(() =>
            {
                var querySource = (typeof(TSource) == typeof(object))
                    ? "counters"
                    : IndexDefinitionHelper.GetQuerySource(Conventions, typeof(TSource), IndexSourceType.Counters);

                if (StringExtensions.IsIdentifier(counter))
                    querySource = $"{querySource}.{counter}";
                else
                    querySource = $"{querySource}[@\"{counter.Replace("\"", "\"\"")}\"]";

                return IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TSource, TReduceResult>(map, Conventions, querySource, translateIdentityProperty: true);
            });
        }

        /// <summary>
        /// Uses reflection to call <see cref="AddMap{TSource}"/> for the base type and all available subclasses.
        /// </summary>
        /// <remarks>This is taken from Oren's code in this thread https://groups.google.com/d/msg/ravendb/eFUlQG-spzE/Ac0PrvsFyJYJ </remarks>
        /// <typeparam name="TBase">The base class type whose descendant types are to be included in the index.</typeparam>
        /// <param name="expr"></param>
        protected void AddMapForAll<TBase>(string counter, Expression<Func<IEnumerable<CounterEntry>, IEnumerable>> map)
        {
            // Index the base class.
            if (typeof(TBase).IsAbstract == false &&
                typeof(TBase).IsInterface == false)
                AddMap<TBase>(counter, map);

            // Index child classes.
            var children = typeof(TBase).Assembly.GetTypes().Where(x => typeof(TBase).IsAssignableFrom(x));
            var addMapGeneric = GetType().GetMethod(nameof(AddMap), BindingFlags.Instance | BindingFlags.NonPublic);
            foreach (var child in children)
            {
                if (child.IsGenericTypeDefinition ||
                    child.IsAbstract ||
                    child.IsInterface)
                    continue;

                var genericEnumerable = typeof(IEnumerable<>).MakeGenericType(child);
                var delegateType = typeof(Func<,>).MakeGenericType(genericEnumerable, typeof(IEnumerable));
                var lambdaExpression = Expression.Lambda(delegateType, map.Body, Expression.Parameter(genericEnumerable, map.Parameters[0].Name));
                addMapGeneric.MakeGenericMethod(child).Invoke(this, new object[] { counter, lambdaExpression });
            }
        }

        /// <summary>
        /// Creates the index definition.
        /// </summary>
        /// <returns></returns>
        public override CountersIndexDefinition CreateIndexDefinition()
        {
            if (Conventions == null)
                Conventions = new DocumentConventions();

            var indexDefinition = new CountersIndexDefinitionBuilder<object, TReduceResult>(IndexName)
            {
                Indexes = Indexes,
                Analyzers = Analyzers,
                Reduce = Reduce,
                Stores = Stores,
                TermVectors = TermVectors,
                SpatialIndexes = SpatialIndexes,
                SuggestionsOptions = IndexSuggestions,
                AnalyzersStrings = AnalyzersStrings,
                IndexesStrings = IndexesStrings,
                StoresStrings = StoresStrings,
                TermVectorsStrings = TermVectorsStrings,
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
            }.ToIndexDefinition(Conventions, validateMap: false);

            foreach (var map in _maps.Select(generateMap => generateMap()))
            {
                string formattedMap = map;
                indexDefinition.Maps.Add(formattedMap);
            }

            return indexDefinition;
        }
    }

    /// <summary>
    /// Allow to create indexes with multiple maps
    /// </summary>
    public abstract class AbstractMultiMapCountersIndexCreationTask : AbstractMultiMapCountersIndexCreationTask<object>
    {
    }
}
