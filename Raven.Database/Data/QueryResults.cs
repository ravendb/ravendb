//-----------------------------------------------------------------------
// <copyright file="QueryResults.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Json.Linq;

namespace Raven.Database.Data
{
	public class QueryResults
	{
		public int LastScannedResult { get; set; }
		public RavenJObject[] Results { get; set; }
		public string[] Errors { get; set; }
		public int TotalResults { get; set; }
	}
}
