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
    public abstract class AbstractIndexDefinitionBuilder<TDocument, TReduceResult, TIndexDefinition> where TIndexDefinition : IndexDefinition, new()
    {
        protected readonly string _indexName;

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
        /// Defines pattern for identifiers of documents which reference IDs of reduce outputs documents
        /// </summary>
        public Expression<Func<TReduceResult, string>> PatternForOutputReduceToCollectionReferences { get; set; }

        /// <summary>
        /// Defines a collection name for reference documents created based on provided pattern
        /// </summary>
        public string PatternReferencesCollectionName { get; set; }

        /// <summary>
        /// Add additional sources to be compiled with the index on the server.
        /// </summary>
        public Dictionary<string, string> AdditionalSources { get; set; }

        public HashSet<AdditionalAssembly> AdditionalAssemblies { get;set;}

        public IndexConfiguration Configuration { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDefinitionBuilder{TDocument,TReduceResult}"/> class.
        /// </summary>
        protected AbstractIndexDefinitionBuilder(string indexName)
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
        public virtual TIndexDefinition ToIndexDefinition(DocumentConventions conventions, bool validateMap = true)
        {
            try
            {
                if (Reduce != null)
                    IndexDefinitionHelper.ValidateReduce(Reduce);

                var indexDefinition = new TIndexDefinition
                {
                    Name = _indexName,
                    Reduce = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(Reduce, conventions, "results", translateIdentityProperty: false),
                    LockMode = LockMode,
                    Priority = Priority,
                    OutputReduceToCollection = OutputReduceToCollection,
                    PatternForOutputReduceToCollectionReferences = PatternReferencesCollectionName,
                    PatternReferencesCollectionName = PatternReferencesCollectionName
                };

                if (PatternForOutputReduceToCollectionReferences != null)
                    indexDefinition.PatternForOutputReduceToCollectionReferences = ConvertPatternForOutputReduceToCollectionReferencesToString(PatternForOutputReduceToCollectionReferences);

                var indexes = ConvertToStringDictionary(Indexes);
                var stores = ConvertToStringDictionary(Stores);
                var analyzers = ConvertToStringDictionary(Analyzers);
                var suggestionsOptions = ConvertToStringSet(SuggestionsOptions).ToDictionary(x => x, x => true);
                var termVectors = ConvertToStringDictionary(TermVectors);
                var spatialOptions = ConvertToStringDictionary(SpatialIndexes);

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

                indexDefinition.AdditionalSources = AdditionalSources;
                indexDefinition.AdditionalAssemblies = AdditionalAssemblies;
                indexDefinition.Configuration = Configuration;

                ToIndexDefinition(indexDefinition, conventions);

                return indexDefinition;
            }
            catch (Exception e)
            {
                throw new IndexCompilationException("Failed to create index " + _indexName, e);
            }
        }

        protected abstract void ToIndexDefinition(TIndexDefinition indexDefinition, DocumentConventions conventions);

        private string ConvertPatternForOutputReduceToCollectionReferencesToString(Expression<Func<TReduceResult, string>> reduceOutputReferencesPattern)
        {
            if (reduceOutputReferencesPattern.Body is MethodCallExpression methodCall)
            {
                // x => $"reports/daily/{x.OrderedAt:yyyy-MM-dd}";
                // x => string.Format("reports/daily/{0:MM/dd/yyyy}", x.OrderedAt);

                if (methodCall.Arguments.Count < 1)
                    throw new InvalidOperationException(
                        $"{nameof(MethodCallExpression)} of {nameof(PatternForOutputReduceToCollectionReferences)} expression must have at least 1 argument");

                if (!(methodCall.Arguments[0] is ConstantExpression stringConstant))
                    throw new InvalidOperationException($"First argument of {nameof(MethodCallExpression)} of {nameof(PatternForOutputReduceToCollectionReferences)} expression must be {nameof(ConstantExpression)}");


                string pattern = stringConstant.Value.ToString();

                for (int i = 1; i < methodCall.Arguments.Count; i++)
                {
                    var expression = methodCall.Arguments[i];

                    RewritePatternParameters(expression, i - 1);
                }

                void RewritePatternParameters(Expression expression, int position)
                {
                    if (expression is UnaryExpression unaryExpression)
                    {
                        if (!(unaryExpression.Operand is MemberExpression memberExpression))
                            throw new InvalidOperationException($"Properties provided in {nameof(PatternForOutputReduceToCollectionReferences)} expression must be {nameof(MemberAccessException)}");

                        pattern = pattern.Replace((position).ToString(), memberExpression.Member.Name);
                    }
                    else if (expression is MemberExpression member)
                    {
                        pattern = pattern.Replace((position).ToString(), member.Member.Name);
                    }
                    else if (expression is NewArrayExpression arrayExpression)
                    {
                        for (int arrayIndex = 0; arrayIndex < arrayExpression.Expressions.Count; arrayIndex++)
                        {
                            var arrayValueExpression = arrayExpression.Expressions[arrayIndex];

                            RewritePatternParameters(arrayValueExpression, arrayIndex);
                        }
                    }
                    else
                    {
                        throw new NotSupportedException($"Unsupported expression in {nameof(PatternForOutputReduceToCollectionReferences)}: '{expression}' (type: {expression.GetType().FullName})");
                    }
                }

                return pattern;
            }

            if (reduceOutputReferencesPattern.Body is BinaryExpression binaryExpression)
            {
                // x => "reports/daily/" + x.OrderedAt;

                string pattern = PatternFromBinary(binaryExpression.Left, binaryExpression.Right);

                string PatternFromBinary(Expression left, Expression right)
                {
                    var result = string.Empty;

                    result += PatternFromExpression(left);
                    result += PatternFromExpression(right);

                    return result;
                }

                string PatternFromExpression(Expression expr)
                {
                    if (expr is ConstantExpression constantExpression)
                    {
                        return constantExpression.Value.ToString();
                    }

                    if (expr is BinaryExpression binaryExpr)
                    {
                        return PatternFromBinary(binaryExpr.Left, binaryExpr.Right);
                    }

                    if (expr is UnaryExpression unaryExpression)
                    {
                        if (!(unaryExpression.Operand is MemberExpression memberExpression))
                            throw new InvalidOperationException($"Properties provided in {nameof(PatternForOutputReduceToCollectionReferences)} expression must be {nameof(MemberAccessException)}");

                        return $"{{{memberExpression.Member.Name}}}";
                    }

                    if (expr is MemberExpression memeExpr)
                    {
                        return $"{{{memeExpr.Member.Name}}}";
                    }

                    throw new NotSupportedException($"Unsupported expression in {nameof(PatternForOutputReduceToCollectionReferences)}: '{expr}'");
                }

                return pattern;
            }

            throw new InvalidOperationException($"Body of {nameof(PatternForOutputReduceToCollectionReferences)} expression must be {nameof(MethodCallExpression)}");

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
    public class IndexDefinitionBuilder<TDocument, TReduceResult> : AbstractIndexDefinitionBuilder<TDocument, TReduceResult, IndexDefinition>
    {
        /// <summary>
        /// Gets or sets the map function
        /// </summary>
        /// <value>The map.</value>
        public Expression<Func<IEnumerable<TDocument>, IEnumerable>> Map { get; set; }

        public IndexDefinitionBuilder(string indexName = null) : base(indexName)
        {
        }

        public override IndexDefinition ToIndexDefinition(DocumentConventions conventions, bool validateMap = true)
        {
            if (Map == null && validateMap)
                throw new InvalidOperationException(string.Format("Map is required to generate an index, you cannot create an index without a valid Map property (in index {0}).", _indexName));

            return base.ToIndexDefinition(conventions, validateMap);
        }

        protected override void ToIndexDefinition(IndexDefinition indexDefinition, DocumentConventions conventions)
        {
            if (Map == null)
                return;

            var querySource = GetQuerySource(conventions);

            var map = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(
                    Map,
                    conventions,
                    querySource,
                    translateIdentityProperty: true);

            indexDefinition.Maps.Add(map);
        }

        private string GetQuerySource(DocumentConventions conventions)
        {
            return (typeof(TDocument) == typeof(object) || ContainsWhereEntityIs())
                ? "docs"
                : IndexDefinitionHelper.GetQuerySource(conventions, typeof(TDocument), IndexSourceType.Documents);
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
