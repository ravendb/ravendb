//-----------------------------------------------------------------------
// <copyright file="IndexDefinition.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Indexing;

namespace Raven.Client.Indexes
{
    /// <summary>
    /// This class provides a way to define a strongly typed index on the client.
    /// </summary>
    public class IndexDefinitionBuilder<TDocument, TReduceResult>
    {
        private readonly string indexName;
        /// <summary>
        /// Gets or sets the map function
        /// </summary>
        /// <value>The map.</value>
        public Expression<Func<IEnumerable<TDocument>, IEnumerable>> Map { get; set; }
        /// <summary>
        /// Gets or sets the reduce function
        /// </summary>
        /// <value>The reduce.</value>
        public Expression<Func<IEnumerable<TReduceResult>, IEnumerable>> Reduce { get; set; }

        /// <summary>
        /// Gets or sets the stores options
        /// </summary>
        /// <value>The stores.</value>
        public IDictionary<Expression<Func<TReduceResult, object>>, FieldStorage> Stores { get; set; }

        /// <summary>
        /// Gets or sets the stores options
        /// </summary>
        /// <value>The stores.</value>
        public IDictionary<string, FieldStorage> StoresStrings { get; set; }

        /// <summary>
        /// Gets or sets the indexing options
        /// </summary>
        /// <value>The indexes.</value>
        public IDictionary<Expression<Func<TReduceResult, object>>, FieldIndexing> Indexes { get; set; }

        /// <summary>
        /// Gets or sets the indexing options
        /// </summary>
        /// <value>The indexes.</value>
        public IDictionary<string, FieldIndexing> IndexesStrings { get; set; }

        /// <summary>
        /// Gets or sets the sort options.
        /// </summary>
        /// <value>The sort options.</value>
        public IDictionary<Expression<Func<TReduceResult, object>>, SortOptions> SortOptions { get; set; }

        /// <summary>
        /// Gets or sets the sort options.
        /// </summary>
        /// <value>The sort options.</value>
        public Dictionary<string, SortOptions> SortOptionsStrings { get; set; }

        /// <summary>
        /// Get os set the analyzers
        /// </summary>
        public IDictionary<Expression<Func<TReduceResult, object>>, string> Analyzers { get; set; }

        /// <summary>
        /// Get os set the analyzers
        /// </summary>
        public IDictionary<string, string> AnalyzersStrings { get; set; }

        /// <summary>
        /// Gets or sets the suggestion options.
        /// </summary>
        /// <value>The suggestion options.</value>
        [Obsolete("Use SuggestionsOptions")]
        public IDictionary<Expression<Func<TReduceResult, object>>, SuggestionOptions> Suggestions
        {
            get { return SuggestionsOptions.ToDictionary(x => x, x => new SuggestionOptions()); }
            set { SuggestionsOptions = value.Keys.ToHashSet(); }
        }

        public ISet<Expression<Func<TReduceResult, object>>> SuggestionsOptions { get; set; }

        /// <summary>
        /// Gets or sets the term vector options
        /// </summary>
        /// <value>The term vectors.</value>
        public IDictionary<Expression<Func<TReduceResult, object>>, FieldTermVector> TermVectors { get; set; }

        /// <summary>
        /// Gets or sets the term vector options
        /// </summary>
        /// <value>The term vectors.</value>
        public IDictionary<string, FieldTermVector> TermVectorsStrings { get; set; }

        /// <summary>
        /// Gets or sets the spatial options
        /// </summary>
        /// <value>The spatial options.</value>
        public IDictionary<Expression<Func<TReduceResult, object>>, SpatialOptions> SpatialIndexes { get; set; }

        /// <summary>
        /// Gets or sets the spatial options
        /// </summary>
        /// <value>The spatial options.</value>
        public IDictionary<string, SpatialOptions> SpatialIndexesStrings { get; set; }

        /// <summary>
        /// Gets or sets the index lock mode
        /// </summary>
        public IndexLockMode LockMode { get; set; }

        /// <summary>
        /// Max number of allowed indexing outputs per one source document
        /// </summary>
        public int? MaxIndexOutputsPerDocument { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDefinitionBuilder{TDocument,TReduceResult}"/> class.
        /// </summary>
        public IndexDefinitionBuilder(string indexName = null)
        {
            this.indexName = indexName ?? GetType().FullName;
            Stores = new Dictionary<Expression<Func<TReduceResult, object>>, FieldStorage>();
            StoresStrings = new Dictionary<string, FieldStorage>();
            Indexes = new Dictionary<Expression<Func<TReduceResult, object>>, FieldIndexing>();
            IndexesStrings = new Dictionary<string, FieldIndexing>();
            SortOptions = new Dictionary<Expression<Func<TReduceResult, object>>, SortOptions>();
            SortOptionsStrings = new Dictionary<string, SortOptions>();
            SuggestionsOptions = new HashSet<Expression<Func<TReduceResult, object>>>();
            Analyzers = new Dictionary<Expression<Func<TReduceResult, object>>, string>();
            AnalyzersStrings = new Dictionary<string, string>();
            TermVectors = new Dictionary<Expression<Func<TReduceResult, object>>, FieldTermVector>();
            TermVectorsStrings = new Dictionary<string, FieldTermVector>();
            SpatialIndexes = new Dictionary<Expression<Func<TReduceResult, object>>, SpatialOptions>();
            SpatialIndexesStrings = new Dictionary<string, SpatialOptions>();
            LockMode = IndexLockMode.Unlock;
        }

        /// <summary>
        /// Toes the index definition.
        /// </summary>
        public IndexDefinition ToIndexDefinition(DocumentConvention convention, bool validateMap = true)
        {
            if (Map == null && validateMap)
                throw new InvalidOperationException(
                    string.Format("Map is required to generate an index, you cannot create an index without a valid Map property (in index {0}).", indexName));

            try
            {
                if (Reduce != null)
                    IndexDefinitionHelper.ValidateReduce(Reduce);

                string querySource = (typeof(TDocument) == typeof(object) || ContainsWhereEntityIs()) ? "docs" : "docs." + convention.GetTypeTagName(typeof(TDocument));
                var indexDefinition = new IndexDefinition
                {
                    Reduce = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(Reduce, convention, "results", translateIdentityProperty: false),
                    LockMode = LockMode
                };

                if (MaxIndexOutputsPerDocument.HasValue)
                    indexDefinition.Configuration.MaxIndexOutputsPerDocument = MaxIndexOutputsPerDocument;

                var indexes = ConvertToStringDictionary(Indexes);
                var stores = ConvertToStringDictionary(Stores);
                var sortOptions = ConvertToStringDictionary(SortOptions);
                var analyzers = ConvertToStringDictionary(Analyzers);
                var suggestionsOptions = ConvertToStringSet(SuggestionsOptions).ToDictionary(x => x, x => true);
                var termVectors = ConvertToStringDictionary(TermVectors);
                var spatialOptions = ConvertToStringDictionary(SpatialIndexes);

                if (convention.PrettifyGeneratedLinqExpressions)
                    indexDefinition.Reduce = IndexPrettyPrinter.TryFormat(indexDefinition.Reduce);

                foreach (var indexesString in IndexesStrings)
                {
                    if (indexes.ContainsKey(indexesString.Key))
                        throw new InvalidOperationException("There is a duplicate key in indexes: " + indexesString.Key);
                    indexes.Add(indexesString);
                }

                foreach (var storeString in StoresStrings)
                {
                    if (stores.ContainsKey(storeString.Key))
                        throw new InvalidOperationException("There is a duplicate key in stores: " + storeString.Key);
                    stores.Add(storeString);
                }

                foreach (var analyzerString in AnalyzersStrings)
                {
                    if (analyzers.ContainsKey(analyzerString.Key))
                        throw new InvalidOperationException("There is a duplicate key in analyzers: " + analyzerString.Key);
                    analyzers.Add(analyzerString);
                }

                foreach (var termVectorString in TermVectorsStrings)
                {
                    if (termVectors.ContainsKey(termVectorString.Key))
                        throw new InvalidOperationException("There is a duplicate key in term vectors: " + termVectorString.Key);
                    termVectors.Add(termVectorString);
                }

                foreach (var spatialString in SpatialIndexesStrings)
                {
                    if (spatialOptions.ContainsKey(spatialString.Key))
                        throw new InvalidOperationException("There is a duplicate key in spatial indexes: " + spatialString.Key);
                    spatialOptions.Add(spatialString);
                }

                foreach (var sortOption in SortOptionsStrings)
                {
                    if (sortOptions.ContainsKey(sortOption.Key))
                        throw new InvalidOperationException("There is a duplicate key in sort options: " + sortOption.Key);
                    sortOptions.Add(sortOption);
                }

                ApplyValues(indexDefinition, indexes, (options, value) => options.Indexing = value);
                ApplyValues(indexDefinition, stores, (options, value) => options.Storage = value);
                ApplyValues(indexDefinition, sortOptions, (options, value) => options.Sort = value);
                ApplyValues(indexDefinition, analyzers, (options, value) => options.Analyzer = value);
                ApplyValues(indexDefinition, termVectors, (options, value) => options.TermVector = value);
                ApplyValues(indexDefinition, spatialOptions, (options, value) => options.Spatial = value);
                ApplyValues(indexDefinition, suggestionsOptions, (options, value) => options.Suggestions = value);

                if (Map != null)
                {
                    var map = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(
                            Map,
                            convention,
                            querySource,
                            translateIdentityProperty: true);

                    indexDefinition.Maps.Add(convention.PrettifyGeneratedLinqExpressions ? IndexPrettyPrinter.TryFormat(map) : map);
                }

                return indexDefinition;
            }
            catch (Exception e)
            {
                throw new IndexCompilationException("Failed to create index " + indexName, e);
            }
        }

        private void ApplyValues<TValue>(IndexDefinition indexDefinition, IDictionary<string, TValue> values, Action<IndexFieldOptions, TValue> action)
        {
            foreach (var kvp in values)
            {
                IndexFieldOptions field;
                if (indexDefinition.Fields.TryGetValue(kvp.Key, out field) == false)
                    indexDefinition.Fields[kvp.Key] = field = new IndexFieldOptions();

                action(field, kvp.Value);
            }
        }

        private bool ContainsWhereEntityIs()
        {
            if (Map == null) return false;
            var whereEntityIsVisitor = new WhereEntityIsVisitor();
            whereEntityIsVisitor.Visit(Map.Body);
            return whereEntityIsVisitor.HasWhereEntityIs;
        }

        private class WhereEntityIsVisitor : ExpressionVisitor
        {
            public bool HasWhereEntityIs { get; set; }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.Name == "WhereEntityIs")
                    HasWhereEntityIs = true;
                return base.VisitMethodCall(node);
            }
        }

        private static IDictionary<string, TValue> ConvertToStringDictionary<TValue>(IEnumerable<KeyValuePair<Expression<Func<TReduceResult, object>>, TValue>> input)
        {
            var result = new Dictionary<string, TValue>();
            foreach (var value in input)
            {
                var propertyPath = value.Key.ToPropertyPath('_');
                result[propertyPath] = value.Value;
            }
            return result;
        }

        private static ISet<string> ConvertToStringSet(IEnumerable<Expression<Func<TReduceResult, object>>> input)
        {
            var result = new HashSet<string>();
            foreach (var value in input)
            {
                var propertyPath = value.ToPropertyPath('_');
                result.Add(propertyPath);
            }
            return result;
        }
    }

    /// <summary>
    /// This class provides a way to define a strongly typed index on the client.
    /// </summary>
    public class IndexDefinitionBuilder<TDocument> : IndexDefinitionBuilder<TDocument, TDocument>
    {
        public IndexDefinitionBuilder(string indexName = null) : base(indexName)
        {
        }
    }
}
