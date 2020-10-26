using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Abstract class used to provide infrastructure service for actual creation tasks
    /// </summary>
    public abstract class AbstractGenericIndexCreationTask<TReduceResult> : AbstractIndexCreationTask
    {
        /// <summary>
        /// Create a new instance
        /// </summary>
        protected AbstractGenericIndexCreationTask()
        {
            Stores = new Dictionary<Expression<Func<TReduceResult, object>>, FieldStorage>();
            StoresStrings = new Dictionary<string, FieldStorage>();
            Indexes = new Dictionary<Expression<Func<TReduceResult, object>>, FieldIndexing>();
            IndexesStrings = new Dictionary<string, FieldIndexing>();
            Analyzers = new Dictionary<Expression<Func<TReduceResult, object>>, string>();
            AnalyzersStrings = new Dictionary<string, string>();
            IndexSuggestions = new HashSet<Expression<Func<TReduceResult, object>>>();
            TermVectors = new Dictionary<Expression<Func<TReduceResult, object>>, FieldTermVector>();
            TermVectorsStrings = new Dictionary<string, FieldTermVector>();
            SpatialIndexes = new Dictionary<Expression<Func<TReduceResult, object>>, SpatialOptions>();
            SpatialIndexesStrings = new Dictionary<string, SpatialOptions>();
        }

        public override bool IsMapReduce => Reduce != null;

        /// <summary>
        /// The reduce definition
        /// </summary>
        protected Expression<Func<IEnumerable<TReduceResult>, IEnumerable>> Reduce { get; set; }

        /// <summary>
        /// Index storage options
        /// </summary>
        protected IDictionary<Expression<Func<TReduceResult, object>>, FieldStorage> Stores { get; set; }

        /// <summary>
        /// Index storage options
        /// </summary>
        protected IDictionary<string, FieldStorage> StoresStrings { get; set; }

        /// <summary>
        /// Index suggest options
        /// </summary>
        protected ISet<Expression<Func<TReduceResult, object>>> IndexSuggestions { get; set; }

        /// <summary>
        /// Index sort options
        /// </summary>
        protected IDictionary<Expression<Func<TReduceResult, object>>, string> Analyzers { get; set; }

        /// <summary>
        /// Index sort options
        /// </summary>
        protected IDictionary<string, string> AnalyzersStrings { get; set; }

        /// <summary>
        /// Index term vector options
        /// </summary>
        protected IDictionary<Expression<Func<TReduceResult, object>>, FieldTermVector> TermVectors { get; set; }

        /// <summary>
        /// Index term vector options
        /// </summary>
        protected IDictionary<string, FieldTermVector> TermVectorsStrings { get; set; }

        /// <summary>
        /// Spatial index options
        /// </summary>
        protected IDictionary<Expression<Func<TReduceResult, object>>, SpatialOptions> SpatialIndexes { get; set; }

        /// <summary>
        /// Spatial index options
        /// </summary>
        protected IDictionary<string, SpatialOptions> SpatialIndexesStrings { get; set; }

        /// <summary>
        /// Indexing options
        /// </summary>
        protected IDictionary<Expression<Func<TReduceResult, object>>, FieldIndexing> Indexes { get; set; }

        /// <summary>
        /// Indexing options
        /// </summary>
        protected IDictionary<string, FieldIndexing> IndexesStrings { get; set; }

        /// <summary>
        /// If not null than each reduce result will be created as a document in the specified collection name.
        /// </summary>
        protected string OutputReduceToCollection { get; set; }

        /// <summary>
        /// Defines pattern for identifiers of documents which reference IDs of reduce outputs documents
        /// </summary>
        protected Expression<Func<TReduceResult, string>> PatternForOutputReduceToCollectionReferences { get; set; }

        /// <summary>
        /// Defines a collection name for reference documents created based on provided pattern
        /// </summary>
        protected string PatternReferencesCollectionName { get; set; }

        /// <summary>
        /// Register a field to be indexed
        /// </summary>
        protected void Index(Expression<Func<TReduceResult, object>> field, FieldIndexing indexing)
        {
            Indexes.Add(field, indexing);
        }

        /// <summary>
        /// Register a field to be indexed
        /// </summary>
        protected void Index(string field, FieldIndexing indexing)
        {
            IndexesStrings.Add(field, indexing);
        }

        /// <summary>
        /// Register a field to be spatially indexed
        /// </summary>
        protected void Spatial(Expression<Func<TReduceResult, object>> field, Func<SpatialOptionsFactory, SpatialOptions> indexing)
        {
            SpatialIndexes.Add(field, indexing(new SpatialOptionsFactory()));
        }

        /// <summary>
        /// Register a field to be spatially indexed
        /// </summary>
        protected void Spatial(string field, Func<SpatialOptionsFactory, SpatialOptions> indexing)
        {
            SpatialIndexesStrings.Add(field, indexing(new SpatialOptionsFactory()));
        }

        /// <summary>
        /// Register a field to be stored
        /// </summary>
        protected void Store(Expression<Func<TReduceResult, object>> field, FieldStorage storage)
        {
            Stores.Add(field, storage);
        }

        protected void StoreAllFields(FieldStorage storage)
        {
            StoresStrings.Add(Constants.Documents.Indexing.Fields.AllFields, storage);
        }

        /// <summary>
        /// Register a field to be stored
        /// </summary>
        protected void Store(string field, FieldStorage storage)
        {
            StoresStrings.Add(field, storage);
        }

        /// <summary>
        /// Register a field to be analyzed
        /// </summary>
        protected void Analyze(Expression<Func<TReduceResult, object>> field, string analyzer)
        {
            Analyzers.Add(field, analyzer);
        }

        /// <summary>
        /// Register a field to be analyzed
        /// </summary>
        protected void Analyze(string field, string analyzer)
        {
            AnalyzersStrings.Add(field, analyzer);
        }

        /// <summary>
        /// Register a field to have term vectors
        /// </summary>
        protected void TermVector(Expression<Func<TReduceResult, object>> field, FieldTermVector termVector)
        {
            TermVectors.Add(field, termVector);
        }

        /// <summary>
        /// Register a field to have term vectors
        /// </summary>
        protected void TermVector(string field, FieldTermVector termVector)
        {
            TermVectorsStrings.Add(field, termVector);
        }

        /// <summary>
        /// Register a field to be sorted
        /// </summary>
        protected void Suggestion(Expression<Func<TReduceResult, object>> field)
        {
            IndexSuggestions.Add(field);
        }

        protected void AddAssembly(AdditionalAssembly assembly)
        {
            if (assembly is null)
                throw new ArgumentNullException(nameof(assembly));

            if (AdditionalAssemblies == null)
                AdditionalAssemblies = new HashSet<AdditionalAssembly>();

            AdditionalAssemblies.Add(assembly);
        }
    }
}
