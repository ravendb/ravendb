using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Database.Indexing;

namespace Raven.Client.Indexes
{

#if !NET_3_5
    /// <summary>
    /// Base class for creating indexes
    /// </summary>
    /// <remarks>
    /// The naming convention is that underscores in the inherited class names are replaced by slashed
    /// For example: Posts_ByName will be saved to Posts/ByName
    /// </remarks>
    [System.ComponentModel.Composition.InheritedExport]
#endif
    public abstract class AbstractIndexCreationTask
    {
        /// <summary>
        /// Creates the index definition.
        /// </summary>
        /// <returns></returns>
        public abstract IndexDefinition CreateIndexDefinition();

        /// <summary>
        /// Gets the name of the index.
        /// </summary>
        /// <value>The name of the index.</value>
        public virtual string IndexName { get { return GetType().Name.Replace("_", "/"); } }

        /// <summary>
        /// Gets or sets the document store.
        /// </summary>
        /// <value>The document store.</value>
        public IDocumentStore DocumentStore { get; private set; }

        /// <summary>
        /// Executes the index creation against the specified document store.
        /// </summary>
        /// <param name="documentStore">The document store.</param>
        public virtual void Execute(IDocumentStore documentStore)
        {
            DocumentStore = documentStore;
            var indexDefinition = CreateIndexDefinition();
            // This code take advantage on the fact that RavenDB will turn an index PUT
            // to a noop of the index already exists and the stored definition matches
            // the new defintion.
            documentStore.DatabaseCommands.PutIndex(IndexName, indexDefinition, true);
        }
    }

    /// <summary>
    /// Base class for creating indexes
    /// </summary>
    /// <remarks>
    /// The naming convention is that underscores in the inherited class names are replaced by slashed
    /// For example: Posts_ByName will be saved to Posts/ByName
    /// </remarks>
    public class AbstractIndexCreationTask<TDocument> :
        AbstractIndexCreationTask<TDocument, TDocument>
    {

    }

    /// <summary>
    /// Base class for creating indexes
    /// </summary>
    /// <remarks>
    /// The naming convention is that underscores in the inherited class names are replaced by slashed
    /// For example: Posts_ByName will be saved to Posts/ByName
    /// </remarks>
    public class AbstractIndexCreationTask<TDocument, TReduceResult> : AbstractIndexCreationTask
    {
        /// <summary>
        /// Creates the index definition.
        /// </summary>
        /// <returns></returns>
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition<TDocument, TReduceResult>
            {
                Indexes = Indexes,
                SortOptions = SortOptions,
                Map = Map,
                Reduce = Reduce,
                ResultTransformer = ResultTransformer,
                Stores = Stores
            }.ToIndexDefinition(DocumentStore.Conventions);
        }

        /// <summary>
        /// The result translator definition
        /// </summary>
        protected Expression<Func<IClientSideDatabase, IEnumerable<TReduceResult>, IEnumerable>> ResultTransformer { get; set; }

        /// <summary>
        /// The reduce defintion
        /// </summary>
        protected Expression<Func<IEnumerable<TReduceResult>, IEnumerable>> Reduce { get; set; }


        /// <summary>
        /// The map defintion
        /// </summary>
        protected Expression<Func<IEnumerable<TDocument>, IEnumerable>> Map { get; set; }


        /// <summary>
        /// Index storage options
        /// </summary>
        protected IDictionary<Expression<Func<TReduceResult, object>>, FieldStorage> Stores
        {
            get;
            set;
        }


        /// <summary>
        /// Index sort options
        /// </summary>
        protected IDictionary<Expression<Func<TReduceResult, object>>, SortOptions> SortOptions
        {
            get;
            set;
        }


        /// <summary>
        /// Indexing options
        /// </summary>
        protected IDictionary<Expression<Func<TReduceResult, object>>, FieldIndexing> Indexes
        {
            get;
            set;
        }

        /// <summary>
        /// Create a new instance
        /// </summary>
        protected AbstractIndexCreationTask()
        {
            Stores = new Dictionary<Expression<Func<TReduceResult, object>>, FieldStorage>();
            Indexes = new Dictionary<Expression<Func<TReduceResult, object>>, FieldIndexing>();
            SortOptions = new Dictionary<Expression<Func<TReduceResult, object>>, SortOptions>();
        }


        /// <summary>
        /// Register a field to be indexeed
        /// </summary>
        protected void Index(Expression<Func<TReduceResult, object>> field, FieldIndexing indexing)
        {
            Indexes.Add(field, indexing);
        }

        /// <summary>
        /// Register a field to be stored
        /// </summary>
        protected void Store(Expression<Func<TReduceResult, object>> field, FieldStorage storage)
        {
            Stores.Add(field, storage);
        }

        /// <summary>
        /// Register a field to be sorted
        /// </summary>
        protected void Sort(Expression<Func<TReduceResult, object>> field, SortOptions sort)
        {
            SortOptions.Add(field, sort);
        }
    }
}