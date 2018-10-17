using System.Collections.Generic;

namespace Raven.Client.Documents.Indexes
{
    public abstract class AbstractJavaScriptIndexCreationTask : AbstractIndexCreationTask
    {
        private readonly IndexDefinition _definition = new IndexDefinition();

        protected AbstractJavaScriptIndexCreationTask()
        {
            _definition.LockMode = IndexLockMode.Unlock;
            _definition.Priority = IndexPriority.Normal;
        }

        /// <summary>
        /// All the map functions for this index
        /// </summary>
        public HashSet<string> Maps
        {
            get => _definition.Maps;
            set => _definition.Maps = value;
        }

        public Dictionary<string, IndexFieldOptions> Fields
        {
            get => _definition.Fields;
            set => _definition.Fields = value;
        }

        protected string Reduce
        {
            get => _definition.Reduce;
            set => _definition.Reduce = value;
        }

        /// <inheritdoc />
        public override bool IsMapReduce => Reduce != null;

        protected string OutputReduceToCollection
        {
            get => _definition.OutputReduceToCollection;
            set => _definition.OutputReduceToCollection = value;
        }

        /// <inheritdoc />
        public override IndexDefinition CreateIndexDefinition()
        {
            _definition.Type = IsMapReduce ? IndexType.JavaScriptMapReduce : IndexType.JavaScriptMap;
            _definition.AdditionalSources = AdditionalSources ?? (_definition.AdditionalSources = new Dictionary<string, string>());
            _definition.Configuration = Configuration;

            return _definition.Clone();
        }
    }
}

