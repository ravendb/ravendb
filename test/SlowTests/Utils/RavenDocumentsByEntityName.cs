using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Indexing;

namespace SlowTests.Utils
{
    ///<summary>
    /// Create an index that allows to tag entities by their entity name
    ///</summary>
    public class RavenDocumentsByEntityName : AbstractIndexCreationTask
    {
        public override bool IsMapReduce
        {
            get { return false; }
        }
        /// <summary>
        /// Return the actual index name
        /// </summary>
        public override string IndexName
        {
            get { return "Raven/DocumentsByEntityName"; }
        }

        /// <summary>
        /// Creates the Raven/DocumentsByEntityName index
        /// </summary>
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Maps = { @"from doc in docs 
select new 
{ 
    Tag = doc[""@metadata""][""Raven-Entity-Name""], 
    LastModified = (DateTime)doc[""@metadata""][""Last-Modified""],
    LastModifiedTicks = ((DateTime)doc[""@metadata""][""Last-Modified""]).Ticks 
};"},
                Fields = {
                    { "Tag" ,new IndexFieldOptions
                {
                    Indexing = FieldIndexing.NotAnalyzed
                } },
                    {
                     "LastModified", new IndexFieldOptions
                        {
                            Indexing = FieldIndexing.NotAnalyzed,
                            Sort = SortOptions.String
                        }
                    },
                    {"LastModifiedTicks",new IndexFieldOptions
                    {
                        Indexing = FieldIndexing.NotAnalyzed,
                        Sort = SortOptions.NumericLong
                    } }
                },


                //DisableInMemoryIndexing = true,
                LockMode = IndexLockMode.LockedIgnore,
            };
        }
    }
}
