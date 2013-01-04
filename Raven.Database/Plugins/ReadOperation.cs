//-----------------------------------------------------------------------
// <copyright file="ReadOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Database.Plugins
{
	public enum ReadOperation
	{
		/// <summary>
		/// Load operation. Load a document by its ID operation.
		/// </summary>
		Load,

		/// <summary>
		/// Query operation. Query documents by a query.
		/// </summary>
		Query,

		/// <summary>
		/// Index operation. Indexing documents.
		/// </summary>
		Index,
	}
}