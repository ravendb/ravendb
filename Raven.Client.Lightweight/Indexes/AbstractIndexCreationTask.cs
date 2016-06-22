//-----------------------------------------------------------------------
// <copyright file="AbstractIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Json.Linq;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Indexes
{
    /// <summary>
    /// Base class for creating indexes
    /// </summary>
    /// <remarks>
    /// The naming convention is that underscores in the inherited class names are replaced by slashed
    /// For example: Posts_ByName will be saved to Posts/ByName
    /// </remarks>
#if !(MONO || DNXCORE50)
    [System.ComponentModel.Composition.InheritedExport]
#endif
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
        public IndexingPriority? Priority { get; set; }


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
        /// Loads the specifed attachment content during the indexing process
        /// </summary>
        [Obsolete("Use RavenFS instead.")]
        public object LoadAttachmentForIndexing(string key)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Executes the index creation against the specified document store in side-by-side mode.
        /// </summary>
        /// <param name="store"></param>
        public void SideBySideExecute(IDocumentStore store, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            store.SideBySideExecuteIndex(this, minimumEtagBeforeReplace, replaceTimeUtc);
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
        /// <param name="databaseCommands"></param>
        /// <param name="documentConvention"></param>
        public virtual void SideBySideExecute(IDatabaseCommands databaseCommands, DocumentConvention documentConvention, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            Conventions = documentConvention;
            var indexDefinition = CreateIndexDefinition();

            var replaceIndexName = Constants.SideBySideIndexNamePrefix + IndexName;
            //check if side by side index exists
            var sideBySideDef = databaseCommands.GetIndex(replaceIndexName);
            if (sideBySideDef != null)
            {
                if (CurrentOrLegacyIndexDefinitionEquals(documentConvention, sideBySideDef, indexDefinition))
                    return;

                UpdateSideBySideIndex(databaseCommands, minimumEtagBeforeReplace, replaceTimeUtc, replaceIndexName, indexDefinition, documentConvention);
                return;
            }

            //check if "regular" index exists
            var serverDef = databaseCommands.GetIndex(IndexName);
            if (serverDef != null)
            {
                if (CurrentOrLegacyIndexDefinitionEquals(documentConvention, serverDef, indexDefinition))
                    return;

                switch (serverDef.LockMode)
                {
                    case IndexLockMode.SideBySide:
                        //keep the SideBySide lock mode from the replaced index
                        indexDefinition.LockMode = IndexLockMode.SideBySide;
                        break;
                    case IndexLockMode.LockedIgnore:
                        //Nothing to do we just ignore this index
                        return;
                    case IndexLockMode.LockedError:
                        throw new InvalidOperationException(string.Format("Can't replace locked index {0} its lock mode is set to: LockedError", serverDef.IndexId));
                }

                UpdateSideBySideIndex(databaseCommands, minimumEtagBeforeReplace, replaceTimeUtc, replaceIndexName, indexDefinition, documentConvention);
            }
            else
            {
                // since index doesn't exist yet - create it in normal mode
                databaseCommands.PutIndex(IndexName, indexDefinition);
                AfterExecute(databaseCommands, documentConvention);
            }
        }

        private void UpdateSideBySideIndex(IDatabaseCommands databaseCommands, Etag minimumEtagBeforeReplace, DateTime? replaceTimeUtc, string replaceIndexName, IndexDefinition indexDefinition, DocumentConvention documentConvention)
        {
            databaseCommands.PutIndex(replaceIndexName, indexDefinition, true);

            databaseCommands
                .Put(Constants.IndexReplacePrefix + replaceIndexName,
                    null,
                    RavenJObject.FromObject(new IndexReplaceDocument { IndexToReplace = IndexName, MinimumEtagBeforeReplace = minimumEtagBeforeReplace, ReplaceTimeUtc = replaceTimeUtc }),
                    new RavenJObject());

            AfterExecute(databaseCommands, documentConvention);
        }

        /// <summary>
        /// Executes the index creation against the specified document database using the specified conventions
        /// </summary>
        public virtual void Execute(IDatabaseCommands databaseCommands, DocumentConvention documentConvention)
        {
            Conventions = documentConvention;
            var indexDefinition = CreateIndexDefinition();

#if !DNXCORE50
            if (documentConvention.PrettifyGeneratedLinqExpressions)
            {
                var serverDef = databaseCommands.GetIndex(IndexName);
                if (serverDef != null && CurrentOrLegacyIndexDefinitionEquals(documentConvention, serverDef, indexDefinition))
                {
                    AfterExecute(databaseCommands, documentConvention);
                    return;
                }
            }
#endif

            // This code take advantage on the fact that RavenDB will turn an index PUT
            // to a noop of the index already exists and the stored definition matches
            // the new definition.
            databaseCommands.PutIndex(IndexName, indexDefinition, true);

            if (Priority != null)
                databaseCommands.SetIndexPriority(IndexName, Priority.Value);

            AfterExecute(databaseCommands, documentConvention);
        }

        public virtual void AfterExecute(IDatabaseCommands databaseCommands, DocumentConvention documentConvention)
        {
            if (documentConvention.IndexAndTransformerReplicationMode.HasFlag(IndexAndTransformerReplicationMode.Indexes))
                ReplicateIndexesIfNeeded(databaseCommands);
        }

        public virtual async Task AfterExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention documentConvention, CancellationToken token = default(CancellationToken))
        {
            if (documentConvention.IndexAndTransformerReplicationMode.HasFlag(IndexAndTransformerReplicationMode.Indexes))
                await ReplicateIndexesIfNeededAsync(asyncDatabaseCommands).ConfigureAwait(false);
        }

        private bool CurrentOrLegacyIndexDefinitionEquals(DocumentConvention documentConvention, IndexDefinition serverDef, IndexDefinition indexDefinition)
        {

            var oldIndexId = serverDef.IndexId;

            try
            {
                serverDef.IndexId = indexDefinition.IndexId;

                if (serverDef.Equals(indexDefinition, false))
                    return true;

                // now we need to check if this is a legacy index...
                var legacyIndexDefinition = GetLegacyIndexDefinition(documentConvention);

                return serverDef.Equals(legacyIndexDefinition, compareIndexIds: false, ignoreFormatting: true, ignoreMaxIndexOutput: true);
            }
            finally
            {
                serverDef.IndexId = oldIndexId;
            }
        }

        private void ReplicateIndexesIfNeeded(IDatabaseCommands databaseCommands)
        {
            var serverClient = databaseCommands as ServerClient;
            if (serverClient == null)
                return;
            var replicateIndexUrl = String.Format("/replication/replicate-indexes?indexName={0}", Uri.EscapeDataString(IndexName));
            using (var replicateIndexRequest = serverClient.CreateRequest(replicateIndexUrl, "POST"))
            {
                try
                {
                    replicateIndexRequest.ExecuteRawResponseAsync().Wait();
                }
                catch (Exception)
                {
                    //ignore errors
                }
            }

        }

        private async Task ReplicateIndexesIfNeededAsync(IAsyncDatabaseCommands databaseCommands)
        {
            var serverClient = databaseCommands as AsyncServerClient;
            if (serverClient == null)
                return;
            var replicateIndexUrl = String.Format("/replication/replicate-indexes?indexName={0}", Uri.EscapeDataString(IndexName));
            using (var replicateIndexRequest = serverClient.CreateRequest(replicateIndexUrl, "POST"))
            {
                try
                {
                    await replicateIndexRequest.ExecuteRawResponseAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // ignore errors
                }
            }

        }


        public IndexDefinition GetLegacyIndexDefinition(DocumentConvention documentConvention)
        {
            IndexDefinition legacyIndexDefinition;
#if !DNXCORE50
            var oldPrettifyGeneratedLinqExpressions = documentConvention.PrettifyGeneratedLinqExpressions;
            documentConvention.PrettifyGeneratedLinqExpressions = false;
#endif
            try
            {
                legacyIndexDefinition = CreateIndexDefinition();
            }
            finally
            {
#if !DNXCORE50
                documentConvention.PrettifyGeneratedLinqExpressions = oldPrettifyGeneratedLinqExpressions;
#endif
            }
            return legacyIndexDefinition;
        }

        /// <summary>
        /// Executes the index creation against the specified document store in side-by-side mode.
        /// </summary>
        public Task SideBySideExecuteAsync(IDocumentStore store, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            return store.SideBySideExecuteIndexAsync(this, minimumEtagBeforeReplace, replaceTimeUtc);
        }

        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        public Task ExecuteAsync(IDocumentStore store)
        {
            return store.ExecuteIndexAsync(this);
        }

        public virtual async Task SideBySideExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention documentConvention, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null, CancellationToken token = default(CancellationToken))
        {
            Conventions = documentConvention;
            var indexDefinition = CreateIndexDefinition();

            var replaceIndexName = Constants.SideBySideIndexNamePrefix + IndexName;
            //check if side by side index exists
            var sideBySideDef = await asyncDatabaseCommands.GetIndexAsync(replaceIndexName, token).ConfigureAwait(false);
            if (sideBySideDef != null)
            {
                if (CurrentOrLegacyIndexDefinitionEquals(documentConvention, sideBySideDef, indexDefinition))
                    return;

                await UpdateSideBySideIndexAsync(asyncDatabaseCommands, minimumEtagBeforeReplace, replaceTimeUtc, token, replaceIndexName, indexDefinition, documentConvention).ConfigureAwait(false);
                return;
            }

            var serverDef = await asyncDatabaseCommands.GetIndexAsync(IndexName, token).ConfigureAwait(false);
            if (serverDef != null)
            {
                if (CurrentOrLegacyIndexDefinitionEquals(documentConvention, serverDef, indexDefinition))
                    return;

                switch (serverDef.LockMode)
                {
                    case IndexLockMode.SideBySide:
                        //keep the SideBySide lock mode from the replaced index
                        indexDefinition.LockMode = IndexLockMode.SideBySide;
                        break;
                    case IndexLockMode.LockedIgnore:
                        //Nothing to do we just ignore this index
                        return;
                    case IndexLockMode.LockedError:
                        throw new InvalidOperationException(string.Format("Can't replace locked index {0} its lock mode is set to: LockedError", serverDef.IndexId));
                }

                await UpdateSideBySideIndexAsync(asyncDatabaseCommands, minimumEtagBeforeReplace, replaceTimeUtc, token, replaceIndexName, indexDefinition, documentConvention).ConfigureAwait(false);
            }
            else
            {
                // since index doesn't exist yet - create it in normal mode
                await asyncDatabaseCommands.PutIndexAsync(IndexName, indexDefinition, token).ConfigureAwait(false);
                await AfterExecuteAsync(asyncDatabaseCommands, documentConvention, token).ConfigureAwait(false);
            }
        }

        private async Task UpdateSideBySideIndexAsync(IAsyncDatabaseCommands asyncDatabaseCommands, Etag minimumEtagBeforeReplace, DateTime? replaceTimeUtc, CancellationToken token, string replaceIndexName, IndexDefinition indexDefinition, DocumentConvention documentConvention)
        {
            await asyncDatabaseCommands.PutIndexAsync(replaceIndexName, indexDefinition, true, token).ConfigureAwait(false);

            await asyncDatabaseCommands
                .PutAsync(Constants.IndexReplacePrefix + replaceIndexName,
                    null,
                    RavenJObject.FromObject(new IndexReplaceDocument { IndexToReplace = IndexName, MinimumEtagBeforeReplace = minimumEtagBeforeReplace, ReplaceTimeUtc = replaceTimeUtc }),
                    new RavenJObject(),
                    token).ConfigureAwait(false);

            await AfterExecuteAsync(asyncDatabaseCommands, documentConvention, token);
        }

        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        public virtual async Task ExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention documentConvention, CancellationToken token = default(CancellationToken))
        {
            Conventions = documentConvention;
            var indexDefinition = CreateIndexDefinition();
#if !DNXCORE50
            if (documentConvention.PrettifyGeneratedLinqExpressions)
            {
                var serverDef = await asyncDatabaseCommands.GetIndexAsync(IndexName, token).ConfigureAwait(false);
                if (serverDef != null && CurrentOrLegacyIndexDefinitionEquals(documentConvention, serverDef, indexDefinition))
                {
                    await AfterExecuteAsync(asyncDatabaseCommands, documentConvention, token).ConfigureAwait(false);
                    return;
                }
            }
#endif

            // This code take advantage on the fact that RavenDB will turn an index PUT
            // to a noop of the index already exists and the stored definition matches
            // the new definition.
            await asyncDatabaseCommands.PutIndexAsync(IndexName, indexDefinition, true, token).ConfigureAwait(false);
            if (Priority != null)
                await asyncDatabaseCommands.SetIndexPriorityAsync(IndexName, Priority.Value, token).ConfigureAwait(false);

            await AfterExecuteAsync(asyncDatabaseCommands, documentConvention, token).ConfigureAwait(false);
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
                Suggestions = IndexSuggestions,
                TermVectors = TermVectors,
                TermVectorsStrings = TermVectorsStrings,
                SpatialIndexes = SpatialIndexes,
                SpatialIndexesStrings = SpatialIndexesStrings,
                DisableInMemoryIndexing = DisableInMemoryIndexing,
                MaxIndexOutputsPerDocument = MaxIndexOutputsPerDocument,
            }.ToIndexDefinition(Conventions);

            var fields = Map.Body.Type.GenericTypeArguments.First().GetProperties();
            foreach (var field in fields)
            {
                if (indexDefinition.SortOptions.ContainsKey(field.Name))
                    continue;
                var fieldType = field.PropertyType;
                if (fieldType == typeof(int))
                {
                    indexDefinition.SortOptions.Add(field.Name, SortOptions.Int);
                }
                else if (fieldType == typeof(long))
                {
                    indexDefinition.SortOptions.Add(field.Name, SortOptions.Long);
                }
                else if (fieldType == typeof(short))
                {
                    indexDefinition.SortOptions.Add(field.Name, SortOptions.Short);
                }
                else if (fieldType == typeof(decimal))
                {
                    indexDefinition.SortOptions.Add(field.Name, SortOptions.Double);
                }
                else if (fieldType == typeof(double))
                {
                    indexDefinition.SortOptions.Add(field.Name, SortOptions.Double);
                }
                else if (fieldType == typeof(float))
                {
                    indexDefinition.SortOptions.Add(field.Name, SortOptions.Float);
                }
            }

            return indexDefinition;
        }

        /// <summary>
        /// Index specific setting that limits the number of map outputs that an index is allowed to create for a one source document. If a map operation applied to
        /// the one document produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document 
        /// will be skipped and the appropriate error message will be added to the indexing errors.
        /// <para>Default value: null means that the global value from Raven configuration will be taken to detect if number of outputs was exceeded.</para>
        /// </summary>
        public int? MaxIndexOutputsPerDocument { get; set; }

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
        protected RavenJObject MetadataFor(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }

        /// <summary>
        /// Allow to access an entity as a document
        /// </summary>
        protected RavenJObject AsDocument(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }
    }
}
