//-----------------------------------------------------------------------
// <copyright file="MultiLoadResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
	/// <summary>
	/// Represent a result which include both document results and included documents
	/// </summary>
	public class MultiLoadResult
	{
		/// <summary>
		/// Gets or sets the document results.
		/// </summary>
		public List<RavenJObject> Results { get; set; }
		/// <summary>
		/// Gets or sets the included documents
		/// </summary>
		public List<RavenJObject> Includes { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="MultiLoadResult"/> class.
		/// </summary>
		public MultiLoadResult()
		{
			Results = new List<RavenJObject>();
			Includes = new List<RavenJObject>();
		}
	}
}
