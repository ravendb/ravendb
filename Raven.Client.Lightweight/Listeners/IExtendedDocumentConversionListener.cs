//-----------------------------------------------------------------------
// <copyright file="IDocumentConversionListener.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.Client.Listeners
{
	/// <summary>
	/// Extended hook for users to provide additional logic for converting to / from 
	/// entities to document/metadata pairs.
	/// </summary>
	public interface IExtendedDocumentConversionListener
	{
		/// <summary>
		/// Called before converting an entity to a document and metadata
		/// </summary>
		void BeforeConversionToDocument(string key, object entity, RavenJObject metadata);

		/// <summary>
		/// Called after having converted an entity to a document and metadata
		/// </summary>
		void AfterConversionToDocument(string key, object entity, RavenJObject document, RavenJObject metadata);

		/// <summary>
		/// Called before converting a document and metadata to an entity
		/// </summary>
		void BeforeConversionToEntity(string key, RavenJObject document, RavenJObject metadata);

		/// <summary>
		/// Called after having converted a document and metadata to an entity
		/// </summary>
		void AfterConversionToEntity(string key, RavenJObject document, RavenJObject metadata, object entity);
	}
}
