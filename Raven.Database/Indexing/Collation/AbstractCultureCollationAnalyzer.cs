//-----------------------------------------------------------------------
// <copyright file="AbstractCultureCollationAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;

namespace Raven.Database.Indexing.Collation
{
	public class AbstractCultureCollationAnalyzer : CollationAnalyzer
	{
		public AbstractCultureCollationAnalyzer()
		{
			var culture = GetType().Name.Replace("CollationAnalyzer","").ToLowerInvariant();
			Init(CultureInfo.GetCultureInfo(culture));
		}
	}
}