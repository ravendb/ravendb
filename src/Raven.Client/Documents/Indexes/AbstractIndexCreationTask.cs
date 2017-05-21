//-----------------------------------------------------------------------
// <copyright file="AbstractIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Util;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Base class for creating indexes
    /// </summary>
    /// <remarks>
    /// The naming convention is that underscores in the inherited class names are replaced by slashed
    /// For example: Posts_ByName will be saved to Posts/ByName
    /// </remarks>
    public abstract class AbstractIndexCreationTask : AbstractCommonApiForIndexesAndTransformers
    {
        /// <summary>
        /// Creates the index definition.
        /// </summary>
        public abstract IndexDefinition CreateIndexDefinition();

        protected internal virtual IEnumerable<object> ApplyReduceFunctionIfExists(IndexQuery indexQuery, IEnumerable<object> enumerable)
        {
            return enumerable.Take(indexQuery.PageSize);
        }

        /// <summary>
        /// Gets a value indicating whether this instance is map reduce index definition
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is map reduce; otherwise, <c>false</c>.
        /// </value>
        public virtual bool IsMapReduce => false;

        /// <summary>
        /// Generates index name from type name replacing all _ with /
        /// <para>e.g.</para>
        /// <para>if our type is <code>'Orders_Totals'</code> then index name would be <code>'Orders/Totals'</code></para>
        /// </summary>
        public virtual string IndexName => GetType().Name.Replace("_", "/");

        /// <summary>
        /// Gets or sets the conventions that should be used when index definition is created.
        /// </summary>
        public DocumentConventions Conventions { get; set; }

        /// <summary>
        ///  index can have a priority that controls how much power of the indexing process it is allowed to consume. index priority can be forced by the user.
        ///  There are four available values that you can set: Normal, Idle, Disabled, Abandoned
        /// <para>Default value: null means that the priority of the index is Normal.</para>
        /// </summary>
        public IndexPriority? Priority { get; set; }

        public IndexLockMode? LockMode { get; set; }

        /// <summary>
        /// Provide a way to dynamically index values with runtime known values
        /// </summary>
        protected object CreateField(string name, object value, bool stored, bool analyzed)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Provide a way to dynamically index values with runtime known values
        /// </summary>
        protected object CreateField(string name, object value)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
        /// </summary>
        /// <param name="lat">Latitude</param>
        /// <param name="lng">Longitude</param>
        /// <returns></returns>
        public static object SpatialGenerate(double? lat, double? lng)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Generate field with values that can be used for spatial clustering on the lat/lng coordinates
        /// </summary>
        public object SpatialClustering(string fieldName, double? lat, double? lng)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Generate field with values that can be used for spatial clustering on the lat/lng coordinates
        /// </summary>
        public object SpatialClustering(string fieldName, double? lat, double? lng,
                                                         int minPrecision,
                                                         int maxPrecision)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
        /// </summary>
        /// <param name="fieldName">The field name, will be used for querying</param>
        /// <param name="lat">Latitude</param>
        /// <param name="lng">Longitude</param>
        /// <returns></returns>
        public static object SpatialGenerate(string fieldName, double? lat, double? lng)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
        /// </summary>
        /// <param name="fieldName">The field name, will be used for querying</param>
        /// <param name="shapeWKT">The shape representation in the WKT format</param>
        /// <returns></returns>
        public static object SpatialGenerate(string fieldName, string shapeWKT)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
        /// </summary>
        /// <param name="fieldName">The field name, will be used for querying</param>
        /// <param name="shapeWKT">The shape representation in the WKT format</param>
        /// <param name="strategy">The spatial strategy to use</param>
        /// <returns></returns>
        public static object SpatialGenerate(string fieldName, string shapeWKT, SpatialSearchStrategy strategy)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
        /// </summary>
        /// <param name="fieldName">The field name, will be used for querying</param>
        /// <param name="shapeWKT">The shape representation in the WKT format</param>
        /// <param name="strategy">The spatial strategy to use</param>
        /// <param name="maxTreeLevel">Maximum number of levels to be used in the PrefixTree, controls the precision of shape representation.</param>
        /// <returns></returns>
        public static object SpatialGenerate(string fieldName, string shapeWKT, SpatialSearchStrategy strategy, int maxTreeLevel)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        public void Execute(IDocumentStore store)
        {
            store.ExecuteIndex(this);
        }

        /// <summary>
        /// Executes the index creation against the specified document database using the specified conventions
        /// </summary>
        public virtual void Execute(IDocumentStore store, DocumentConventions conventions)
        {
            PutIndex(store, conventions);
        }

        private void PutIndex(IDocumentStore store, DocumentConventions conventions)
        {
            AsyncHelpers.RunSync(() => PutIndexAsync(store, conventions));
        }

        private Task PutIndexAsync(IDocumentStore store, DocumentConventions conventions, CancellationToken token = default(CancellationToken))
        {
            Conventions = conventions;

            var indexDefinition = CreateIndexDefinition();
            indexDefinition.Name = IndexName;

            if (LockMode.HasValue)
                indexDefinition.LockMode = LockMode.Value;

            if (Priority.HasValue)
                indexDefinition.Priority = Priority.Value;

            return store.Admin.SendAsync(new PutIndexesOperation(indexDefinition), token);
        }

        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        public Task ExecuteAsync(IDocumentStore store)
        {
            return store.ExecuteIndexAsync(this);
        }

        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        public virtual Task ExecuteAsync(IDocumentStore store, DocumentConventions conventions, CancellationToken token = default(CancellationToken))
        {
            return PutIndexAsync(store, conventions, token: token);
        }
    }

    /// <summary>
    /// Base class for creating indexes
    /// </summary>
    public class AbstractIndexCreationTask<TDocument> :
        AbstractIndexCreationTask<TDocument, TDocument>
    {
    }

    /// <summary>
    /// Base class for creating indexes
    /// </summary>
    public class AbstractIndexCreationTask<TDocument, TReduceResult> : AbstractGenericIndexCreationTask<TReduceResult>
    {
        /// <summary>
        /// Creates the index definition.
        /// </summary>
        /// <returns></returns>
        public override IndexDefinition CreateIndexDefinition()
        {
            if (Conventions == null)
                Conventions = new DocumentConventions();

            var indexDefinition = new IndexDefinitionBuilder<TDocument, TReduceResult>(IndexName)
            {
                Indexes = Indexes,
                IndexesStrings = IndexesStrings,
                SortOptionsStrings = IndexSortOptionsStrings,
                SortOptions = IndexSortOptions,
                Analyzers = Analyzers,
                AnalyzersStrings = AnalyzersStrings,
                Map = Map,
                Reduce = Reduce,
                Stores = Stores,
                StoresStrings = StoresStrings,
                SuggestionsOptions = IndexSuggestions,
                TermVectors = TermVectors,
                TermVectorsStrings = TermVectorsStrings,
                SpatialIndexes = SpatialIndexes,
                SpatialIndexesStrings = SpatialIndexesStrings,
                OutputReduceToCollection = OutputReduceToCollection,
            }.ToIndexDefinition(Conventions);

            var fields = Map.Body.Type.GenericTypeArguments.First().GetProperties();
            foreach (var field in fields)
            {
                IndexFieldOptions options;
                if (indexDefinition.Fields.TryGetValue(field.Name, out options) == false)
                    indexDefinition.Fields[field.Name] = options = new IndexFieldOptions();

                if (options.Sort.HasValue)
                    continue;

                var fieldType = field.PropertyType;
                switch (DocumentConventions.GetRangeType(fieldType))
                {
                    case RangeType.Double:
                    case RangeType.Long:
                        options.Sort = SortOptions.Numeric;
                        break;
                }
            }

            return indexDefinition;
        }

        /// <summary>
        /// Gets a value indicating whether this instance is map reduce index definition
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is map reduce; otherwise, <c>false</c>.
        /// </value>
        public override bool IsMapReduce => Reduce != null;

        /// <summary>
        /// The map definition
        /// </summary>
        protected Expression<Func<IEnumerable<TDocument>, IEnumerable>> Map { get; set; }
    }

    public abstract class AbstractCommonApiForIndexesAndTransformers
    {
        /// <summary>
        /// Allows to use lambdas recursively
        /// </summary>
        protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, TResult> func)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Allows to use lambdas recursively
        /// </summary>
        protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, IEnumerable<TResult>> func)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Allows to use lambdas recursively
        /// </summary>
        protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, ICollection<TResult>> func)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Allows to use lambdas recursively
        /// </summary>
        protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, ISet<TResult>> func)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Allows to use lambdas recursively
        /// </summary>
        protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, HashSet<TResult>> func)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Allows to use lambdas recursively
        /// </summary>
        protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, SortedSet<TResult>> func)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Loads the specified document during the indexing process
        /// </summary>
        public T LoadDocument<T>(string key)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Loads the specified document during the indexing process
        /// </summary>
        public T[] LoadDocument<T>(IEnumerable<string> keys)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Allows to use lambdas recursively
        /// </summary>
        protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, IList<TResult>> func)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Allows to use lambdas recursively
        /// </summary>
        protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, TResult[]> func)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Allows to use lambdas recursively
        /// </summary>
        protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, List<TResult>> func)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Allow to get to the metadata of the document
        /// </summary>
        protected JsonObject.Metadata MetadataFor(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }

        /// <summary>
        /// Allow to access an entity as a document
        /// </summary>
        protected JsonObject AsJson(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }
    }
}
