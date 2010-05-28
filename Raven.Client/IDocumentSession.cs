using Raven.Client.Linq;

namespace Raven.Client
{
	public interface IDocumentSession : IInMemoryDocumentSessionOperations
	{
		T Load<T>(string id);

		T[] Load<T>(params string[] ids);

		void Refresh<T>(T entity);

		IRavenQueryable<T> Query<T>(string indexName);

		IDocumentQuery<T> LuceneQuery<T>(string indexName);

		void SaveChanges();
	}
}
