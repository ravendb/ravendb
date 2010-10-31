using Raven.Client.Document;
using Raven.Client.Shard;

namespace Raven.Client
{
	/// <summary>
	/// Extensions that provide nicer API for using Raven Client API
	/// </summary>
	public static class RavenExtensions
	{
		/// <summary>
		/// Constant for the builtin index 
		/// </summary>
		public const string RavenDocumentByEntityName = "Raven/DocumentsByEntityName";

		/// <summary>
		/// Query the "Raven/DocumentsByEntityName" index for all instances of a specified tag.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="session">The session.</param>
		/// <returns></returns>
		public static IDocumentQuery<T> LuceneQuery<T>(this ISyncAdvancedSessionOperation session)
		{
			var shardedDocumentSession = session as ShardedDocumentSession;
			if(shardedDocumentSession != null)
			{
				var documentQuery = (ShardedDocumentQuery<T>)shardedDocumentSession.LuceneQuery<T>(RavenDocumentByEntityName);
				documentQuery.ForEachQuery((documentSession, query) => query.Where(GenerateQuery<T>(documentSession.Advanced)));
				return documentQuery;
			}

            return session.LuceneQuery<T>(RavenDocumentByEntityName)
				.Where(GenerateQuery<T>(session));
		}

		private static string GenerateQuery<T>(ISyncAdvancedSessionOperation session)
		{
			return "Tag:[[" + session.Conventions.GetTypeTagName(typeof(T)) +"]]";
		}
	}
}
