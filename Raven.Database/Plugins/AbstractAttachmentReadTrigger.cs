using System.ComponentModel.Composition;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Plugins
{
    [InheritedExport]
    public class AbstractAttachmentReadTrigger : IRequiresDocumentDatabaseInitialization
    {
        /// <summary>
        ///  Ask the trigger whatever the document should be read by the user.
        ///  </summary><remarks>
        ///  The document and metadata instances SHOULD NOT be modified.
        ///  </remarks>
        /// <param name="key">The key of the read document - can be null if reading a projection</param>
        /// <param name="data">The attachment data being read</param>
        /// <param name="metadata">The document metadata</param>
        /// <param name="operation">Whatever the operation is a load or a query</param>
        /// <returns>
        ///  * If the result is Allow, the operation contiues as usual. 
        ///  * If the result is Deny, the opeartion will return an error to the user 
        ///    if asking for a particular document, or an error document in place of 
        ///    the result if asking for a query.
        ///  * If the result is Ignore, the operation will return null to the user if
        ///    asking for a particular document, or skip including the result entirely 
        ///    in the query results.
        ///  </returns>
        public virtual ReadVetoResult AllowRead(string key, byte[] data, JObject metadata, ReadOperation operation)
        {
            return ReadVetoResult.Allowed;
        }

        /// <summary>
        ///  Allow the trigger the option of modifying the document and metadata instances
        ///  before the user can see them. 
        ///  </summary><remarks>
        ///  The modified values are transient, and are NOT saved to the database.
        ///  </remarks><param name="key">The key of the read document - can be null if reading a projection</param>
        /// <param name="data">The attachment being read</param>
        /// <param name="metadata">The document metadata</param>
        /// <param name="operation">Whatever the operation is a load or a query</param>
        public virtual byte[] OnRead(string key, byte[] data, JObject metadata, ReadOperation operation)
        {
            return data;
        }

        public void Initialize(DocumentDatabase database)
        {
            Database = database;
            Initialize();
        }

        public virtual void Initialize()
        {
        }

        public DocumentDatabase Database { get; set; }
        
    }
}