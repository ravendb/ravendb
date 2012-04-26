namespace Raven.Client.UniqueConstraints
{
	using System;
	using System.Linq.Expressions;

	using Raven.Json.Linq;

	public static class UniqueConstraintExtensions
	{
		public static T GetByUniqueConstraint<T>(this IDocumentSession session, Expression<Func<T, object>> keySelector, object value)
		{
			var typeName = session.Advanced.DocumentStore.Conventions.GetTypeTagName(typeof(T));
			var body = (MemberExpression)keySelector.Body;
			var propertyName = body.Member.Name;

			var constraintDoc = session.Include("Id").Load("UniqueConstraints/" + typeName.ToLowerInvariant() + "/" + propertyName.ToLowerInvariant() + "/" + value) as RavenJObject;

			if (constraintDoc != null)
			{
				RavenJToken idValue = null;

				constraintDoc.TryGetValue("Id", out idValue);

				if (idValue != null)
				{
					return session.Load<T>(idValue.ToString());
				}

			}

			return default(T);
		}
	}
}
