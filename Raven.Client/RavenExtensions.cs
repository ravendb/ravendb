using Raven.Client.Document;
using Raven.Client.Shard;

namespace Raven.Client
{
	public static class RavenExtensions
	{
		private const string Raven_DocumentByEntityName = "Raven/DocumentsByEntityName";

		public static IDocumentQuery<T> Query<T>(this IDocumentSession session)
		{
			var shardedDocumentSession = session as ShardedDocumentSession;
			if(shardedDocumentSession != null)
			{
				var documentQuery = (ShardedDocumentQuery<T>)shardedDocumentSession.Query<T>(Raven_DocumentByEntityName);
				documentQuery.ForEachQuery((documentSession, query) => query.Where(GenerateQuery<T>(session)));
				return documentQuery;
			}

			return session.Query<T>(Raven_DocumentByEntityName)
				.Where(GenerateQuery<T>(session));
		}

		private static string GenerateQuery<T>(IDocumentSession session)
		{
			return "Tag:`" + session.Conventions.GetTypeTagName(typeof(T)) +"`";
		}
	}
}