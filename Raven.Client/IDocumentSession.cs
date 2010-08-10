using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;

namespace Raven.Client
{
	public interface IDocumentSession : IInMemoryDocumentSessionOperations
	{
		IDatabaseCommands DatabaseCommands { get; }

		T Load<T>(string id);

		T[] Load<T>(params string[] ids);

		void Refresh<T>(T entity);

		IRavenQueryable<T> Query<T>(string indexName);

		IRavenQueryable<T> Query<T, TIndexCreator>(string indexName) where TIndexCreator : AbstractIndexCreationTask, new();

		IDocumentQuery<T> LuceneQuery<T>(string indexName);

		ILoaderWithInclude Include(string path);

		void SaveChanges();

        //It's a bit messier, but this has to be declared here, see link below for an explanation
        //http://stackoverflow.com/questions/3071634/strange-behaviour-when-using-dynamic-types-as-method-parameters  
        //(swap IExtendedInterface for IDocumentSession and IActualInterface for IInMemorydocumentSessionOperations)

        //The problem is that the session variable is ISession, but Store(..) doesn't exist in/on that interface
        //C# handles this when we're not calling Store(..) with a dynamic value (i.e. resolved at run-time)

        //This is the best way I can think of doing it?
#if !NET_3_5        
        new void Store(dynamic entity);
#endif
    }
}
