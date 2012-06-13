using Raven.Client.Document;

namespace Raven.Client.Linq
{
	///<summary>
	/// Generate a new document query
	///</summary>
	public interface IDocumentQueryGenerator
	{
		/// <summary>
		/// Gets the conventions associated with this query
		/// </summary>
		DocumentConvention Conventions { get; }

		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		IDocumentQuery<T> Query<T>(string indexName);

#if !NET35
		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName);
#endif
	}

}