//-----------------------------------------------------------------------
// <copyright file="AbstractIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Extensions;
using Raven.Client.Util;

namespace Raven.Client.Documents.Indexes
{
    public abstract class AbstractIndexCreationTask : AbstractIndexCreationTaskBase<IndexDefinition>
    {
        /// <summary>
        /// Allow to get to the metadata of the document
        /// </summary>
        protected IJsonObject.IMetadata MetadataFor(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }

        /// <summary>
        /// Allow to get attachments of the document (without binary data)
        /// </summary>
        protected IEnumerable<AttachmentName> AttachmentsFor(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }

        /// <summary>
        /// Allow to get counter names of the document
        /// </summary>
        protected IEnumerable<string> CounterNamesFor(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }

        /// <summary>
        /// Allow to get timeseries names of the document
        /// </summary>
        protected IEnumerable<string> TimeSeriesNamesFor(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }

        /// <summary>
        /// Allows to retrieve attachment of the document
        /// </summary>
        public IAttachmentObject LoadAttachment(object doc, string name)
        {
            throw new NotSupportedException("This is here as a marker only");
        }

        /// <summary>
        /// Allows to retrieve attachments of the document
        /// </summary>
        public IEnumerable<IAttachmentObject> LoadAttachments(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }

        /// <summary>
        /// Allow to access an entity as a document
        /// </summary>
        protected IJsonObject AsJson(object doc)
        {
            throw new NotSupportedException("This is here as a marker only");
        }
    }
    /// <summary>
    /// Base class for creating indexes
    /// </summary>
    /// <remarks>
    /// The naming convention is that underscores in the inherited class names are replaced by slashed
    /// For example: Posts_ByName will be saved to Posts/ByName
    /// </remarks>
    public abstract class AbstractIndexCreationTaskBase<TIndexDefinition> : AbstractCommonApiForIndexes, IAbstractIndexCreationTask where TIndexDefinition : IndexDefinition
    {
        IndexPriority? IAbstractIndexCreationTask.Priority => Priority;

        string IAbstractIndexCreationTask.IndexName => IndexName;

        IndexDeploymentMode? IAbstractIndexCreationTask.DeploymentMode => DeploymentMode;

        DocumentConventions IAbstractIndexCreationTask.Conventions
        {
            get => Conventions;
            set => Conventions = value;
        }

        IndexDefinition IAbstractIndexCreationTask.CreateIndexDefinition()
        {
            return CreateIndexDefinition();
        }

        void IAbstractIndexCreationTask.Execute(IDocumentStore store, DocumentConventions conventions, string database)
        {
            Execute(store, conventions, database);
        }

        Task IAbstractIndexCreationTask.ExecuteAsync(IDocumentStore store, DocumentConventions conventions, string database, CancellationToken token)
        {
            return ExecuteAsync(store, conventions, database, token);
        }

        /// <summary>
        /// Creates the index definition.
        /// </summary>
        public abstract TIndexDefinition CreateIndexDefinition();

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

        public IndexDeploymentMode? DeploymentMode { get; set; }
        
        /// <summary>
        ///  Set search engine for index.
        ///<para>Default value: null means that the configuration from the database will be used.</para>
        /// </summary>
        public SearchEngineType? SearchEngineType { get; set; }
        
        /// <summary>
        /// Set whether archived, unarchived or all documents will be indexed
        /// </summary>
        public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }

        /// <summary>
        /// Index state
        /// </summary>
        public IndexState? State { get; set; }

        /// <summary>
        /// Provide a way to dynamically index values with runtime known values
        /// </summary>
        protected object CreateField(
            string name,
            object value,
            CreateFieldOptions options)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

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
        /// Generates a vector field in the index, generating embeddings from the provided value
        /// </summary>
        /// <param name="value">Source of text to generate tokens</param>
        public object VectorSearch(string value)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <inheritdoc cref="VectorSearch(string)"/>
        /// <param name="values">Enumerable of text to generate tokens</param>
        public object VectorSearch(IEnumerable<string> values)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }
        
        /// <summary>
        /// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
        /// </summary>
        /// <param name="lat">Latitude</param>
        /// <param name="lng">Longitude</param>
        public object CreateSpatialField(double? lat, double? lng)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
        /// </summary>
        /// <param name="shapeWkt">The shape representation in the WKT format</param>
        public object CreateSpatialField(string shapeWkt)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }
        
        /// <summary>
        /// Creates a vector field in the index, generating embeddings from the provided value
        /// </summary>
        /// <param name="value">Source of text to generate tokens</param>
        public object CreateVectorSearch(string value) => throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        
        /// <summary>
        /// Creates a vector field in the index, generating embeddings from the provided value
        /// </summary>
        /// <param name="value">Source of texts to generate tokens</param>
        public object CreateVectorSearch(IEnumerable<string> value) => throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        
        /// <summary>
        /// Creates a vector field in the index from the provided value
        /// </summary>
        /// <param name="value">Source of embedding</param>
        public object CreateVector(IEnumerable<float> value) => throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        
        /// <summary>
        /// Creates a vector field in the index from the provided values
        /// </summary>
        /// <param name="value">Source of embeddings</param>
        public object CreateVector(IEnumerable<IEnumerable<float>> value) => throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        
        /// <inheritdoc cref="CreateVector(System.Collections.Generic.IEnumerable{float})"/>
        public object CreateVector(IEnumerable<sbyte> value) => throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        
        /// <inheritdoc cref="CreateVector(System.Collections.Generic.IEnumerable{System.Collections.Generic.IEnumerable{float}})"/>
        public object CreateVector(IEnumerable<IEnumerable<sbyte>> value) => throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        
        /// <inheritdoc cref="CreateVector(System.Collections.Generic.IEnumerable{float})"/>
        public object CreateVector(IEnumerable<byte> value) => throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        
        /// <inheritdoc cref="CreateVector(System.Collections.Generic.IEnumerable{System.Collections.Generic.IEnumerable{float}})"/>
        public object CreateVector(IEnumerable<IEnumerable<byte>> value) => throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        
        /// <inheritdoc cref="CreateVector(System.Collections.Generic.IEnumerable{float})"/>
        public object CreateVector(string value) => throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        
        /// <inheritdoc cref="CreateVector(System.Collections.Generic.IEnumerable{System.Collections.Generic.IEnumerable{float}})"/>
        public object CreateVector(IEnumerable<string> value) => throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        
        /// <summary>
        /// Executes the index creation against the specified document database using the specified conventions
        /// </summary>
        public virtual void Execute(IDocumentStore store, DocumentConventions conventions = null, string database = null)
        {
            AsyncHelpers.RunSync(() => ExecuteAsync(store, conventions, database));
        }
        
        /// <summary>
        /// Expert: List of compound fields that Corax can use to optimize certain queries
        /// </summary>
        public List<string[]> CompoundFieldsStrings { get; set; }
        
        protected void CompoundField(string firstField, string secondField)
        {
            CompoundFieldsStrings ??= new();
            CompoundFieldsStrings.Add(new[]{firstField, secondField});
        }
        
        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        public virtual Task ExecuteAsync(IDocumentStore store, DocumentConventions conventions = null, string database = null, CancellationToken token = default)
        {
            database = store.GetDatabase(database);

            var oldConventions = Conventions;

            try
            {
                Conventions = conventions ?? Conventions ?? store.GetRequestExecutor(database).Conventions;

                var indexDefinition = CreateIndexDefinition();
                indexDefinition.Name = IndexName;

                if (LockMode.HasValue)
                    indexDefinition.LockMode = LockMode.Value;

                if (Priority.HasValue)
                    indexDefinition.Priority = Priority.Value;

                if (State.HasValue)
                    indexDefinition.State = State.Value;
                
                if (ArchivedDataProcessingBehavior.HasValue)
                    indexDefinition.ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior;
                
                if (DeploymentMode.HasValue)
                    indexDefinition.DeploymentMode = DeploymentMode.Value;

                return store.Maintenance.ForDatabase(database).SendAsync(new PutIndexesOperation(indexDefinition), token);
            }
            finally
            {
                Conventions = oldConventions;
            }
        }
    }

    public interface IAbstractIndexCreationTask
    {
        string IndexName { get; }

        IndexPriority? Priority { get; }

        IndexState? State { get; }

        IndexDeploymentMode? DeploymentMode { get; }

        DocumentConventions Conventions { get; set; }

        SearchEngineType? SearchEngineType { get; }

        IndexDefinition CreateIndexDefinition();

        void Execute(IDocumentStore store, DocumentConventions conventions = null, string database = null);

        Task ExecuteAsync(IDocumentStore store, DocumentConventions conventions = null, string database = null, CancellationToken token = default);
    }

    /// <summary>
    /// Base class for creating indexes
    /// </summary>
    public abstract class AbstractIndexCreationTask<TDocument> : AbstractIndexCreationTask<TDocument, TDocument>
    {
    }

    /// <summary>
    /// Base class for creating indexes
    /// </summary>
    public abstract class AbstractIndexCreationTask<TDocument, TReduceResult> : AbstractGenericIndexCreationTask<TReduceResult>
    {
        /// <summary>
        /// Creates the index definition.
        /// </summary>
        /// <returns></returns>
        public override IndexDefinition CreateIndexDefinition()
        {
            if (Conventions == null)
                Conventions = new DocumentConventions();

            IndexDefinitionBuilder<TDocument, TReduceResult> builder = new(IndexName)
            {
                Indexes = Indexes,
                IndexesStrings = IndexesStrings,
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
                VectorIndexes = VectorIndexes,
                SpatialIndexesStrings = SpatialIndexesStrings,
                VectorIndexesStrings = VectorIndexesStrings,
                OutputReduceToCollection = OutputReduceToCollection,
                PatternForOutputReduceToCollectionReferences = PatternForOutputReduceToCollectionReferences,
                PatternReferencesCollectionName = PatternReferencesCollectionName,
                AdditionalSources = AdditionalSources,
                AdditionalAssemblies = AdditionalAssemblies,
                Configuration = Configuration,
                LockMode = LockMode,
                Priority = Priority,
                State = State,
                DeploymentMode = DeploymentMode,
                CompoundFieldsStrings = CompoundFieldsStrings,
                CompoundFields = CompoundFields,
                ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior
            };

            if (SearchEngineType.HasValue)
            {
                builder.Configuration[Constants.Configuration.Indexes.IndexingStaticSearchEngineType] = SearchEngineType.Value.ToString();
            }

            var indexDefinition = builder.ToIndexDefinition(Conventions);

            return indexDefinition;
        }

        /// <summary>
        /// The map definition
        /// </summary>
        protected Expression<Func<IEnumerable<TDocument>, IEnumerable>> Map { get; set; }
    }

    public abstract class AbstractCommonApiForIndexes : ILoadCommonApiForIndexes, ILoadCompareExchangeApiForIndexes
    {
        protected AbstractCommonApiForIndexes()
        {
            Configuration = new IndexConfiguration();
            NoTracking = new NoTrackingCommonApiForIndexes();
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

        public NoTrackingCommonApiForIndexes NoTracking { get; }

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

        protected T? TryConvert<T>(object value) where T : struct
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        /// <summary>
        /// Add additional sources to be compiled with the index on the server.
        /// </summary>
        public Dictionary<string, string> AdditionalSources { get; set; }

        public HashSet<AdditionalAssembly> AdditionalAssemblies { get; set; }

        public IndexConfiguration Configuration { get; set; }

        public T LoadDocument<T>(string id)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public T LoadDocument<T>(string id, string collectionName)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public T[] LoadDocument<T>(IEnumerable<string> ids)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public T[] LoadDocument<T>(IEnumerable<string> ids, string collectionName)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public T LoadCompareExchangeValue<T>(string key)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public T[] LoadCompareExchangeValue<T>(IEnumerable<string> keys)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }
        
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
        /// <summary>
        /// Allow to index DateOnly as Ticks
        /// </summary>
        protected DateOnly AsDateOnly(object field)
        {
            throw new NotSupportedException("This is here as a marker only");
        }

        /// <summary>
        /// Allow to index TimeOnly as Ticks
        /// </summary>
        protected TimeOnly AsTimeOnly(object field)
        {
            throw new NotSupportedException("This is here as a marker only");
        }
#endif
    }

    public sealed class NoTrackingCommonApiForIndexes : ILoadCommonApiForIndexes
    {
        public T LoadDocument<T>(string id)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public T LoadDocument<T>(string id, string collectionName)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public T[] LoadDocument<T>(IEnumerable<string> ids)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }

        public T[] LoadDocument<T>(IEnumerable<string> ids, string collectionName)
        {
            throw new NotSupportedException("This can only be run on the server side");
        }
    }

    public interface ILoadCompareExchangeApiForIndexes
    {
        /// <summary>
        /// Loads the specified compare exchange value during the indexing process
        /// </summary>
        public T LoadCompareExchangeValue<T>(string key);

        /// <summary>
        /// Loads the specified compare exchange values during the indexing process
        /// </summary>
        public T[] LoadCompareExchangeValue<T>(IEnumerable<string> keys);
    }

    public interface ILoadCommonApiForIndexes
    {
        /// <summary>
        /// Loads the specified document during the indexing process
        /// </summary>
        public T LoadDocument<T>(string id);

        /// <summary>
        /// Loads the specified document during the indexing process
        /// </summary>
        public T LoadDocument<T>(string id, string collectionName);

        /// <summary>
        /// Loads the specified document during the indexing process
        /// </summary>
        public T[] LoadDocument<T>(IEnumerable<string> ids);

        /// <summary>
        /// Loads the specified document during the indexing process
        /// </summary>
        public T[] LoadDocument<T>(IEnumerable<string> ids, string collectionName);
    }
}
