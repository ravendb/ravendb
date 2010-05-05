using System.ComponentModel.Composition;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Plugins
{
	/// <summary>
	/// * Read triggers may be called on projections from indexes, not just documents
	/// * Read triggers do NOT run while indexing documents.
	/// </summary>
	[InheritedExport]
	public interface IReadTrigger 
	{
		/// <summary>
		/// Ask the trigger whatever the document should be read by the user.
		/// </summary>
		/// <remarks>
		/// The document and metadata instances SHOULD NOT be modified.
		/// </remarks>
		/// <param name="document">The document being read</param>
		/// <param name="metadata">The document metadata</param>
		/// <param name="operation">Whatever the operation is a load or a query</param>
		/// <returns>
		/// * If the result is Allow, the operation contiues as usual. 
		/// * If the result is Deny, the opeartion will return an error to the user 
		///   if asking for a particular document, or an error document in place of 
		///   the result if asking for a query.
		/// * If the result is Ignore, the operation will return null to the user if
		///   asking for a particular document, or skip including the result entirely 
		///   in the query results.
		/// </returns>
		ReadVetoResult AllowRead(JObject document, JObject metadata, ReadOperation operation);

		/// <summary>
		/// Allow the trigger the option of modifying the document and metadata instances
		/// before the user can see them. 
		/// </summary>
		/// <remarks>
		/// The modified values are transient, and are NOT saved to the database.
		/// </remarks>
		/// <param name="document">The document being read</param>
		/// <param name="metadata">The document metadata</param>
		/// <param name="operation">Whatever the operation is a load or a query</param>
		void OnRead(JObject document, JObject metadata, ReadOperation operation);
	}
}