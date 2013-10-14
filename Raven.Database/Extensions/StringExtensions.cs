// -----------------------------------------------------------------------
//  <copyright file="StringExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Database.Extensions
{
	public static class StringExtensions
	{
		public static List<string> GetSemicolonSeparatedValues(this string self)
		{
			return self.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
			.Select(x => x.Trim())
			.ToList();
		}
	}
}