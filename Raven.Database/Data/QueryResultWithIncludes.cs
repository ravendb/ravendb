// -----------------------------------------------------------------------
//  <copyright file="QueryResultWithIncludes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Data
{
	public class QueryResultWithIncludes : QueryResult
	{
		public HashSet<string> IdsToInclude { get; set; }
	}
}