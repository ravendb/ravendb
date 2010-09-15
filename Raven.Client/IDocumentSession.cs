using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;

namespace Raven.Client
{
	/// <summary>
	/// Interface for document session
	/// </summary>
	public interface IDocumentSession : IInMemoryDocumentSessionOperations
	{
		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		IDatabaseCommands DatabaseCommands { get; }

		/// <summary>
		/// Loads the specified entity with the specified id.
		/// </summary>
		/// <param name="id">The id.</param>
		T Load<T>(string id);

		/// <summary>
		/// Loads the specified entities with the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		T[] Load<T>(params string[] ids);

		/// <summary>
		/// Refreshes the specified entity from Raven server.
		/// </summary>
		/// <param name="entity">The entity.</param>
		void Refresh<T>(T entity);

		/// <summary>
		/// Queries the specified index using Linq.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <param name="indexName">Name of the index.</param>
		IRavenQueryable<T> Query<T>(string indexName);

		/// <summary>
		/// Queries the index specified by <see cref="TIndexCreator"/> using Linq.
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		/// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
		/// <returns></returns>
		IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

		/// <summary>
		/// Query the specified index using Lucene syntax
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		IDocumentQuery<T> LuceneQuery<T>(string indexName);

		ILoaderWithInclude Include(string path);

		/// <summary>
		/// Gets the document URL for the specified entity.
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <returns></returns>
		string GetDocumentUrl(object entity);

		/// <summary>
		/// Saves all the changes to the Raven server.
		/// </summary>
		void SaveChanges();

        //It's a bit messier, but this has to be declared here, see link below for an explanation
        //http://stackoverflow.com/questions/3071634/strange-behaviour-when-using-dynamic-types-as-method-parameters  
        //(swap IExtendedInterface for IDocumentSession and IActualInterface for IInMemorydocumentSessionOperations)

        //The problem is that the session variable is ISession, but Store(..) doesn't exist in/on that interface
        //C# handles this when we're not calling Store(..) with a dynamic value (i.e. resolved at run-time)

        //This is the best way I can think of doing it?
#if !NET_3_5        
		/// <summary>
		/// Stores the specified dynamic entity.
		/// </summary>
		/// <param name="entity">The entity.</param>
        new void Store(dynamic entity);
#endif
	}
}
