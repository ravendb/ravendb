//-----------------------------------------------------------------------
// <copyright file="EntityToDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.Client
{
	/// <summary>
	/// Delegate definition for converting an entity to a document and metadata
	/// </summary>
	public delegate void EntityToDocument(object entity, RavenJObject document, RavenJObject metadata);

	/// <summary>
	/// Delegate definition for converting a document and metadata to an entity
	/// </summary>
	public delegate void DocumentToEntity(object entity, RavenJObject document, RavenJObject metadata);
}
