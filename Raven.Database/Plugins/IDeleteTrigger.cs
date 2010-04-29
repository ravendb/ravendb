using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public interface IDeleteTrigger
	{
		/// <summary>
		/// Ask the trigger whatever the DELETE should be vetoed.
		/// If the trigger vote to veto the DELETE, it needs to provide a human readable 
		/// explanation why the DELETE was rejected.
		/// </summary>
		/// <remarks>
		/// This method SHOULD NOT modify either the document or the metadata.
		/// </remarks>
		/// <param name="key">The document key</param>
		/// <returns>Whatever the put was vetoed or not</returns>
		VetoResult AllowDelete(string key);

		/// <summary>
		/// Allow the trigger to perform any logic just before the document is deleted.
		/// </summary>
		/// <remarks>
		/// If the trigger need to access the previous state of the document, the trigger should
		/// implement <seealso cref="IRequiresDocumentDatabaseInitialization"/> and use the provided
		/// <seealso cref="DocumentDatabase"/> instance to Get it. The returned result would be the old
		/// document (if it exists) or null.
		/// Any call to the provided <seealso cref="DocumentDatabase"/> instance will be done under the
		/// same transaction as the DELETE operation.
		/// </remarks>
		/// <param name="key">The document key</param>
		void OnDelete(string key);

		/// <summary>
		/// Allow the trigger to perform any logic _after_ the transaction was committed.
		/// For example, by notifying interested parties.
		/// </summary>
		/// <param name="key">The document key</param>
		void AfterCommit(string key);
	}
}