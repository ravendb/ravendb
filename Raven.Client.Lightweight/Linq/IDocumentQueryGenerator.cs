namespace Raven.Client.Linq
{
	///<summary>
	/// Generate a new document query
	///</summary>
	public interface IDocumentQueryGenerator
	{
		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		IDocumentQuery<T> Query<T>(string indexName);
	}

	///<summary>
	/// Generate a new asynchronous document query
	///</summary>
	public interface IAsyncDocumentQueryGenerator
	{
		/// <summary>
		/// Create a new asynchronous query for <typeparam name="T"/>
		/// </summary>
		IAsyncDocumentQuery<T> Query<T>(string indexName);
	}
}