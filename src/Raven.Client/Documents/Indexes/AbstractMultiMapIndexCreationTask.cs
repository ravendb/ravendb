using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Allow to create indexes with multiple maps
    /// </summary>
    public abstract class AbstractMultiMapIndexCreationTask<TReduceResult> : AbstractGenericIndexCreationTask<TReduceResult>
    {
        private readonly List<Func<string>> _maps = new List<Func<string>>();

        protected void AddMap<TSource>(Expression<Func<IEnumerable<TSource>, IEnumerable>> map)
        {
            if (map == null)
                throw new ArgumentNullException(nameof(map));

            _maps.Add(() =>
            {
                string querySource = typeof(TSource) == typeof(object)
                    ? "docs"
                    : IndexDefinitionHelper.GetQuerySource(Conventions, typeof(TSource), IndexSourceType.Documents);

                return IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TSource, TReduceResult>(map, Conventions, querySource, translateIdentityProperty: true);
            });
        }

        /// <summary>
        /// Uses reflection to call <see cref="AddMap{TSource}"/> for the base type and all available subclasses.
        /// </summary>
        /// <remarks>This is taken from Oren's code in this thread https://groups.google.com/d/msg/ravendb/eFUlQG-spzE/Ac0PrvsFyJYJ </remarks>
        /// <typeparam name="TBase">The base class type whose descendant types are to be included in the index.</typeparam>
        /// <param name="map"></param>
        protected void AddMapForAll<TBase>(Expression<Func<IEnumerable<TBase>, IEnumerable>> map)
        {
            if (map == null)
                throw new ArgumentNullException(nameof(map));

            // Index the base class.
            if (typeof(TBase).IsAbstract == false &&
                typeof(TBase).IsInterface == false)
                AddMap(map);

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
                addMapGeneric.MakeGenericMethod(child).Invoke(this, new[] { lambdaExpression });
            }
        }

        /// <summary>
        /// Creates the index definition.
        /// </summary>
        /// <returns></returns>
        public override IndexDefinition CreateIndexDefinition()
        {
            if (Conventions == null)
                Conventions = new DocumentConventions();

            var indexDefinition = new IndexDefinitionBuilder<object, TReduceResult>(IndexName)
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
                PatternReferencesCollectionName = PatternReferencesCollectionName,
                AdditionalSources = AdditionalSources,
                AdditionalAssemblies = AdditionalAssemblies,
                Configuration = Configuration,
                LockMode = LockMode,
                Priority = Priority
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
    public abstract class AbstractMultiMapIndexCreationTask : AbstractMultiMapIndexCreationTask<object>
    {
    }
}
