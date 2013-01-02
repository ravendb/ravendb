//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	/// <summary>
	/// The result of a PUT operation
	/// </summary>
	public class PutResult
	{
		/// <summary>
		/// Gets or sets the key.
		/// </summary>
		/// <value>The key.</value>
		public string Key { get; set; }
		/// <summary>
		/// Gets or sets the generated Etag for the PUT operation
		/// </summary>
		/// <value>The Etag.</value>
		public Etag ETag { get; set; }
	}
}
