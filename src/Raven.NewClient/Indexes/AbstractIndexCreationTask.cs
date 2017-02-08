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
using Newtonsoft.Json.Linq;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Data.Indexes;
using Raven.NewClient.Operations;
using Raven.NewClient.Operations.Databases.Indexes;
using Sparrow.Json;
using Raven.NewClient.Client.Commands;

namespace Raven.NewClient.Client.Indexes
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
        public virtual bool IsMapReduce { get { return false; } }

        /// <summary>
        /// Generates index name from type name replacing all _ with /
        /// <para>e.g.</para>
        /// <para>if our type is <code>'Orders_Totals'</code> then index name would be <code>'Orders/Totals'</code></para>
        /// </summary>
        public virtual string IndexName { get { return GetType().Name.Replace("_", "/"); } }

        /// <summary>
        /// Gets or sets the conventions that should be used when index definition is created.
        /// </summary>
        public DocumentConvention Conventions { get; set; }

        /// <summary>
        ///  index can have a priority that controls how much power of the indexing process it is allowed to consume. index priority can be forced by the user.
        ///  There are four available values that you can set: Normal, Idle, Disabled, Abandoned
        /// <para>Default value: null means that the priority of the index is Normal.</para>
        /// </summary>
        public IndexPriority? Priority { get; set; }


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

        [Obsolete]
        protected class SpatialIndex
        {
            /// <summary>
            /// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
            /// </summary>
            /// <param name="fieldName">The field name, will be used for querying</param>
            /// <param name="lat">Latitude</param>
            /// <param name="lng">Longitude</param>
            [Obsolete("Use SpatialGenerate instead.")]
            public static object Generate(string fieldName, double? lat, double? lng)
            {
                throw new NotSupportedException("This method is provided solely to allow query translation on the server");
            }

            /// <summary>
            /// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
            /// </summary>
            /// <param name="lat">Latitude</param>
            /// <param name="lng">Longitude</param>
            [Obsolete("Use SpatialGenerate instead.")]
            public static object Generate(double? lat, double? lng)
            {
                throw new NotSupportedException("This method is provided solely to allow query translation on the server");
            }
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
        /// Executes the index creation against the specified document store in side-by-side mode.
        /// </summary>
        /// <param name="store"></param>
        /// <param name="minimumEtagBeforeReplace">The minimum etag after which indexes will be swapped.</param>
        public void SideBySideExecute(IDocumentStore store, long? minimumEtagBeforeReplace = null)
        {
            store.SideBySideExecuteIndex(this, minimumEtagBeforeReplace);
        }

        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        public void Execute(IDocumentStore store)
        {
            store.ExecuteIndex(this);
        }

        /// <summary>
        /// Executes the index creation using in side-by-side mode.
        /// </summary>
        /// <param name="documentConvention"></param>
        /// <param name="minimumEtagBeforeReplace">The minimum etag after which indexes will be swapped.</param>
        public virtual void SideBySideExecute(DocumentStoreBase documentStore, DocumentConvention documentConvention, long? minimumEtagBeforeReplace = null)
        {
            PutIndex(documentStore, documentConvention, minimumEtagBeforeReplace);
        }

        /// <summary>
        /// Executes the index creation against the specified document database using the specified conventions
        /// </summary>
        public virtual void Execute(DocumentStoreBase documentStore, DocumentConvention documentConvention)
        {
            PutIndex(documentStore, documentConvention);
        }

        private void PutIndex(DocumentStoreBase documentStore, DocumentConvention documentConvention, long? minimumEtagBeforeReplace = null)
        {
            Conventions = documentConvention;
            var indexDefinition = CreateIndexDefinition();

            var requestExecuter = documentStore.GetRequestExecuter();

            JsonOperationContext context;
            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var admin = new AdminOperationExecuter(documentStore, requestExecuter, context);
                indexDefinition.MinimumEtagBeforeReplace = minimumEtagBeforeReplace;
                var putIndexOperation = new PutIndexOperation(IndexName, indexDefinition);
                admin.Send(putIndexOperation);

                if (Priority != null)
                {
                    var setIndexPriority = new SetIndexPriorityOperation(IndexName, Priority.Value);
                    admin.Send(setIndexPriority);
                }
            }
        }

        private async Task PutIndexAsync(DocumentStoreBase documentStore, DocumentConvention documentConvention, long? minimumEtagBeforeReplace = null)
        {
            Conventions = documentConvention;
            var indexDefinition = CreateIndexDefinition();

            var requestExecuter = documentStore.GetRequestExecuter();

            JsonOperationContext context;
            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var admin = new AdminOperationExecuter(documentStore, requestExecuter, context);
                indexDefinition.MinimumEtagBeforeReplace = minimumEtagBeforeReplace;
                var putIndexOperation = new PutIndexOperation(IndexName, indexDefinition);

                await admin.SendAsync(putIndexOperation).ConfigureAwait(false);

                if (Priority != null)
                {
                    var setIndexPriority = new SetIndexPriorityOperation(IndexName, Priority.Value);
                    await admin.SendAsync(setIndexPriority).ConfigureAwait(false);
                }
            }
        }

        public IndexDefinition GetLegacyIndexDefinition(DocumentConvention documentConvention)
        {
            IndexDefinition legacyIndexDefinition;
            var oldPrettifyGeneratedLinqExpressions = documentConvention.PrettifyGeneratedLinqExpressions;
            documentConvention.PrettifyGeneratedLinqExpressions = false;
            try
            {
                legacyIndexDefinition = CreateIndexDefinition();
            }
            finally
            {
                documentConvention.PrettifyGeneratedLinqExpressions = oldPrettifyGeneratedLinqExpressions;
            }
            return legacyIndexDefinition;
        }

        /// <summary>
        /// Executes the index creation against the specified document store in side-by-side mode.
        /// </summary>
        public Task SideBySideExecuteAsync(IDocumentStore store, long? minimumEtagBeforeReplace = null)
        {
            return store.SideBySideExecuteIndexAsync(this, minimumEtagBeforeReplace);
        }

        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        public Task ExecuteAsync(IDocumentStore store)
        {
            return store.ExecuteIndexAsync(this);
        }

        public virtual async Task SideBySideExecuteAsync(DocumentStoreBase documentStore, DocumentConvention documentConvention, long? minimumEtagBeforeReplace = null, CancellationToken token = default(CancellationToken))
        {
            await PutIndexAsync(documentStore, documentConvention, minimumEtagBeforeReplace).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        public virtual async Task ExecuteAsync(DocumentStoreBase documentStore, DocumentConvention documentConvention, CancellationToken token = default(CancellationToken))
        {
            await PutIndexAsync(documentStore, documentConvention).ConfigureAwait(false);
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
        protected internal override IEnumerable<object> ApplyReduceFunctionIfExists(IndexQuery indexQuery, IEnumerable<object> enumerable)
        {
            if (Reduce == null)
                return enumerable.Take(indexQuery.PageSize);

            return Conventions.ApplyReduceFunction(GetType(), typeof(TReduceResult), enumerable, () =>
            {
                var compile = Reduce.Compile();
                return (objects => compile(objects.Cast<TReduceResult>()));
            }).Take(indexQuery.PageSize);
        }

        /// <summary>
        /// Creates the index definition.
        /// </summary>
        /// <returns></returns>
        public override IndexDefinition CreateIndexDefinition()
        {
            if (Conventions == null)
                Conventions = new DocumentConvention();

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
                SpatialIndexesStrings = SpatialIndexesStrings
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
                if (fieldType == typeof(int))
                {
                    options.Sort = SortOptions.NumericDefault;
                }
                else if (fieldType == typeof(long))
                {
                    options.Sort = SortOptions.NumericDefault;
                }
                else if (fieldType == typeof(short))
                {
                    options.Sort = SortOptions.NumericDefault;
                }
                else if (fieldType == typeof(decimal))
                {
                    options.Sort = SortOptions.NumericDefault;
                }
                else if (fieldType == typeof(double))
                {
                    options.Sort = SortOptions.NumericDefault;
                }
                else if (fieldType == typeof(float))
                {
                    options.Sort = SortOptions.NumericDefault;
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
        public override bool IsMapReduce
        {
            get { return Reduce != null; }
        }

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
        /// Loads the specifed document during the indexing process
        /// </summary>
        public T LoadDocument<T>(string key)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Loads the specifed document during the indexing process
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
        protected JObject MetadataFor(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }

        /// <summary>
        /// Allow to access an entity as a document
        /// </summary>
        protected JObject AsDocument(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }
    }
}
