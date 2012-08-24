// -----------------------------------------------------------------------
//  <copyright file="QueryResultWithIncludes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Data
{
	public class QueryResultWithIncludes : QueryResult
	{
		[JsonIgnore]
		public HashSet<string> IdsToInclude { get; set; }
	}
}