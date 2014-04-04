using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
    /// <summary>
    /// Implementers of this class are called whenever an object is returned from a map operation on a map reduce index.
    /// </summary>
    [InheritedExport]
    public abstract class AbstractMapOnMapReduceTrigger : IRequiresDocumentDatabaseInitialization
    {
        public void Initialize(DocumentDatabase database)
        {
            Database = database;
            Initialize();
        }

        public virtual void Initialize()
        {
			
        }

        public virtual void SecondStageInit()
        {

        }


        public abstract AbstractMapOnMapReduceTriggerBatcher CreateBatcher(string indexName);

        public DocumentDatabase Database { get; set; }
    }
}