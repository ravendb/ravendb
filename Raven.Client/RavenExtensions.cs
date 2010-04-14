using Raven.Client.Util;

namespace Raven.Client
{
	public static class RavenExtensions
	{
		public static IDocumentQuery<T> Query<T>(this IDocumentSession session)
		{
			var whereClause = "Tag:" + Inflector.Pluralize(typeof(T).Name);
            return session.Query<T>("Raven/DocumentsByEntityName")
				.Where(whereClause);
		}
	}
}