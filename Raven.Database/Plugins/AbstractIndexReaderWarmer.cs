using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using Lucene.Net.Index;

namespace Raven.Database.Plugins
{
    /// <summary>
    /// Allows warming of an indexreader before it is returned by the indexwriter
    /// </summary>
    [InheritedExport]
    public abstract class AbstractIndexReaderWarmer : IRequiresDocumentDatabaseInitialization
    {
        public void Initialize(DocumentDatabase database)
        {
            Database = database;
            Initialize();
        }

        public void SecondStageInit()
        {
        }

        public virtual void Initialize()
        {

        }

        public DocumentDatabase Database { get; set; }

        public abstract void WarmIndexReader(string indexName, IndexReader indexReader);
    }
}
