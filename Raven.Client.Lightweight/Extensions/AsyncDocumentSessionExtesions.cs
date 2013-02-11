using System;
using Raven.Abstractions.Data;

namespace Raven.Client.Extensions
{
	/// <summary>
	/// Extension methods that allow the usage of non-async Store methods directly on an Async Session
	/// </summary>
	public static class AsyncDocumentSessionExtesions
	{
		/// <summary>
		/// Stores the specified entity with the specified etag.
		/// The entity will be saved when <see cref="IAsyncDocumentSession.SaveChangesAsync"/> is called.
		/// </summary>
		public static void Store(this IAsyncDocumentSession session, object entity, Etag etag)
		{
			session.Advanced.Store(entity, etag);
		}

		/// <summary>
		/// Stores the specified entity in the session. The entity will be saved when <see cref="IAsyncDocumentSession.SaveChangesAsync"/> is called.
		/// </summary>
		/// <param name="entity">The entity.</param>
		public static void Store(this IAsyncDocumentSession session, object entity)
		{
			session.Advanced.Store(entity);
		}

		/// <summary>
		/// Stores the specified entity with the specified etag, under the specified id
		/// </summary>
		public static void Store(this IAsyncDocumentSession session, object entity, string id)
		{
			session.Advanced.Store(entity, id);
		}

		/// <summary>
		/// Stores the specified dynamic entity, under the specified id
		/// </summary>
		/// <param name="entity">The entity.</param>
		/// <param name="id">The id to store this entity under. If other entity exists with the same id it will be overridden.</param>
		public static void Store(this IAsyncDocumentSession session, object entity, Etag etag, string id)
		{
			session.Advanced.Store(entity, etag, id);
		}
	}
}
