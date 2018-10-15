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
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// This class provides a way to define a strongly typed index on the client.
    /// </summary>
    public class IndexDefinitionBuilder<TDocument, TReduceResult>
    {
        private readonly string _indexName;
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
        /// Get or set the analyzers
        /// </summary>
        public IDictionary<Expression<Func<TReduceResult, object>>, string> Analyzers { get; set; }

        /// <summary>
        /// Get or set the analyzers
        /// </summary>
        public IDictionary<string, string> AnalyzersStrings { get; set; }

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
        public IndexLockMode? LockMode { get; set; }

        /// <summary>
        /// Gets or sets the index lock mode
        /// </summary>
        public IndexPriority? Priority { get; set; }

        /// <summary>
        /// If not null than each reduce result will be created as a document in the specified collection name.
        /// </summary>
        public string OutputReduceToCollection { get; set; }

        /// <summary>
        /// Add additional sources to be compiled with the index on the server.
        /// </summary>
        public Dictionary<string, string> AdditionalSources { get; set; }

        public IndexConfiguration Configuration { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDefinitionBuilder{TDocument,TReduceResult}"/> class.
        /// </summary>
        public IndexDefinitionBuilder(string indexName = null)
        {
            _indexName = indexName ?? DocumentConventions.DefaultGetCollectionName(GetType());
            if (_indexName.Length > 256)
                throw new ArgumentException("The index name is limited to 256 characters, but was: " + _indexName, nameof(indexName));

            Stores = new Dictionary<Expression<Func<TReduceResult, object>>, FieldStorage>();
            StoresStrings = new Dictionary<string, FieldStorage>();
            Indexes = new Dictionary<Expression<Func<TReduceResult, object>>, FieldIndexing>();
            IndexesStrings = new Dictionary<string, FieldIndexing>();
            SuggestionsOptions = new HashSet<Expression<Func<TReduceResult, object>>>();
            Analyzers = new Dictionary<Expression<Func<TReduceResult, object>>, string>();
            AnalyzersStrings = new Dictionary<string, string>();
            TermVectors = new Dictionary<Expression<Func<TReduceResult, object>>, FieldTermVector>();
            TermVectorsStrings = new Dictionary<string, FieldTermVector>();
            SpatialIndexes = new Dictionary<Expression<Func<TReduceResult, object>>, SpatialOptions>();
            SpatialIndexesStrings = new Dictionary<string, SpatialOptions>();
            Configuration = new IndexConfiguration();
        }

        /// <summary>
        /// Toes the index definition.
        /// </summary>
        public IndexDefinition ToIndexDefinition(DocumentConventions conventions, bool validateMap = true)
        {
            if (Map == null && validateMap)
                throw new InvalidOperationException(
                    string.Format("Map is required to generate an index, you cannot create an index without a valid Map property (in index {0}).", _indexName));

            try
            {
                if (Reduce != null)
                    IndexDefinitionHelper.ValidateReduce(Reduce);

                string querySource = (typeof(TDocument) == typeof(object) || ContainsWhereEntityIs()) ? "docs" : "docs." + conventions.GetCollectionName(typeof(TDocument));
                var indexDefinition = new IndexDefinition
                {
                    Name = _indexName,
                    Reduce = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(Reduce, conventions, "results", translateIdentityProperty: false),
                    LockMode = LockMode,
                    Priority = Priority,
                    OutputReduceToCollection = OutputReduceToCollection
                };

                var indexes = ConvertToStringDictionary(Indexes);
                var stores = ConvertToStringDictionary(Stores);
                var analyzers = ConvertToStringDictionary(Analyzers);
                var suggestionsOptions = ConvertToStringSet(SuggestionsOptions).ToDictionary(x => x, x => true);
                var termVectors = ConvertToStringDictionary(TermVectors);
                var spatialOptions = ConvertToStringDictionary(SpatialIndexes);

                if (conventions.PrettifyGeneratedLinqExpressions)
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



                ApplyValues(indexDefinition, indexes, (options, value) => options.Indexing = value);
                ApplyValues(indexDefinition, stores, (options, value) => options.Storage = value);
                ApplyValues(indexDefinition, analyzers, (options, value) => options.Analyzer = value);
                ApplyValues(indexDefinition, termVectors, (options, value) => options.TermVector = value);
                ApplyValues(indexDefinition, spatialOptions, (options, value) => options.Spatial = value);
                ApplyValues(indexDefinition, suggestionsOptions, (options, value) => options.Suggestions = value);

                if (Map != null)
                {
                    var map = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(
                            Map,
                            conventions,
                            querySource,
                            translateIdentityProperty: true);

                    indexDefinition.Maps.Add(conventions.PrettifyGeneratedLinqExpressions ? IndexPrettyPrinter.TryFormat(map) : map);
                }

                indexDefinition.AdditionalSources = AdditionalSources;
                indexDefinition.Configuration = Configuration;

                return indexDefinition;
            }
            catch (Exception e)
            {
                throw new IndexCompilationException("Failed to create index " + _indexName, e);
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
            if (Map == null)
                return false;
            var whereEntityIsVisitor = new WhereEntityIsVisitor();
            whereEntityIsVisitor.Visit(Map.Body);
            return whereEntityIsVisitor.HasWhereEntityIs;
        }

        private class WhereEntityIsVisitor : ExpressionVisitor
        {
            public bool HasWhereEntityIs { get; private set; }

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
