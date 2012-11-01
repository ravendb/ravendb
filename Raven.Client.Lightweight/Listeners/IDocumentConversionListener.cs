//-----------------------------------------------------------------------
// <copyright file="IDocumentConversionListener.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.Client.Listeners
{
	/// <summary>
	/// Hook for users to provide additional logic for converting to / from 
	/// entities to document/metadata pairs.
	/// </summary>
	public interface IDocumentConversionListener
	{
		/// <summary>
		/// Called when converting an entity to a document and metadata
		/// </summary>
		void EntityToDocument(object entity, RavenJObject document, RavenJObject metadata);

		/// <summary>
		/// Called when converting a document and metadata to an entity
		/// </summary>
		void DocumentToEntity(object entity, RavenJObject document, RavenJObject metadata);

	}
}