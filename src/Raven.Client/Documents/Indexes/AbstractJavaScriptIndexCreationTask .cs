using System.Collections.Generic;

namespace Raven.Client.Documents.Indexes
{
    public abstract class AbstractJavaScriptIndexCreationTask : AbstractIndexCreationTask
    {
        private IndexDefinition _definition = new IndexDefinition();

        public AbstractJavaScriptIndexCreationTask()
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

        public IndexConfiguration Configuration
        {
            get => _definition.Configuration;
            set => _definition.Configuration = value;
        }

        /// <summary>
        /// Gets a value indicating whether this instance is map reduce index definition
        /// </summary>
        /// <value>
        /// 	<c>true</c> if this instance is map reduce; otherwise, <c>false</c>.
        /// </value>
        public override bool IsMapReduce => Reduce != null;


        public override IndexDefinition CreateIndexDefinition()
        {
            _definition.Type = IsMapReduce ? IndexType.JavaScriptMapReduce : IndexType.JavaScriptMap;
            _definition.AdditionalSources = AdditionalSources ?? (_definition.AdditionalSources = new Dictionary<string, string>()) ;
            return _definition.Clone();
        }

    }
}

