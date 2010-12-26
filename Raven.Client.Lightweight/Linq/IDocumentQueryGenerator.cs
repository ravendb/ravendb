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
		IDocumentQuery<T> Query<T>();
	}
}