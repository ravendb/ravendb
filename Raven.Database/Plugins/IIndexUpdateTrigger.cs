using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
    /// <summary>
    /// Implementors of this interface are called whenever an index entry is 
    /// created / deleted.
    /// Work shouldn't be done by the methods of this interface, rather, they
    /// should be done in a background thread. Communication between threads can
    /// use either in memory data structure or the persistent (and transactional )
    /// queue implementation available on the transactional storage.
    /// </summary>
    /// <remarks>
    /// * All operations are delete/create operations, whatever the value
    ///   previously existed or not.
    /// * It is possible for OnIndexEntryDeleted to be called for non existant
    ///   values.
    /// * It is possible for a single entry key to be called inserted several times
    ///   entry keys are NOT unique.
    /// </remarks>
    [InheritedExport]
    public interface IIndexUpdateTrigger
    {
        /// <summary>
        /// Notify that a document with the specified key was deleted
        /// Key may represent a mising document
        /// </summary>
        /// <param name="indexName">The updated index name</param>
        /// <param name="entryKey">The entry key</param>
        void OnIndexEntryDeleted(string indexName, string entryKey);

        /// <summary>
        /// Notify that the specifid document with the specified key is about 
        /// to be inserted.
        /// </summary>
        /// <remarks>
        /// You may modify the provided lucene document, changes made to the document
        /// will be written to the Lucene index
        /// </remarks>
        /// <param name="indexName">The updated index name</param>
        /// <param name="entryKey">The entry key</param>
        /// <param name="document">The lucene document about to be written</param>
        void OnIndexEntryCreated(string indexName, string entryKey, Lucene.Net.Documents.Document document);
    }
}