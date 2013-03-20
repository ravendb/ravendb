//-----------------------------------------------------------------------
// <copyright file="TermsQueryRunnerExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Database.Queries
{
	public static class TermsQueryRunnerExtensions
	{
		public static ISet<string> ExecuteGetTermsQuery(this DocumentDatabase self, string index, string field, string fromValue, int pageSize)
		{
			return new TermsQueryRunner(self).GetTerms(index, field, fromValue, Math.Min(pageSize, self.Configuration.MaxPageSize));
		}
	}
}