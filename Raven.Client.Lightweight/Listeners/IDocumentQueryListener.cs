namespace Raven.Client.Listeners
{
	/// <summary>
	/// Hook for users to modify all queries globally
	/// </summary>
	public interface IDocumentQueryListener
	{
		/// <summary>
		/// Allow to customize a query globally
		/// </summary>
		void BeforeQueryExecuted(IDocumentQueryCustomization queryCustomization);
	}
}