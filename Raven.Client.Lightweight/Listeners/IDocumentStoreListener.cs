//-----------------------------------------------------------------------
// <copyright file="IDocumentStoreListener.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.Client.Listeners
{
	/// <summary>
	/// Hook for users to provide additional logic on store operations
	/// </summary>
	public interface IDocumentStoreListener
	{
		/// <summary>
		/// Invoked before the store request is sent to the server.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="entityInstance">The entity instance.</param>
		/// <param name="metadata">The metadata.</param>
		/// <param name="original">The original document that was loaded from the server</param>
		/// <returns>
		/// Whatever the entity instance was modified and requires us re-serialize it.
		/// Returning true would force re-serialization of the entity, returning false would 
		/// mean that any changes to the entityInstance would be ignored in the current SaveChanges call.
		/// </returns>
		bool BeforeStore(string key, object entityInstance, RavenJObject metadata, RavenJObject original);

		/// <summary>
		/// Invoked after the store request is sent to the server.
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="entityInstance">The entity instance.</param>
		/// <param name="metadata">The metadata.</param>
		void AfterStore(string key, object entityInstance, RavenJObject metadata);
	}
}