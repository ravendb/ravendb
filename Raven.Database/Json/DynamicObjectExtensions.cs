//-----------------------------------------------------------------------
// <copyright file="DynamicObjectExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Lucene.Net.Documents;

namespace Raven.Database.Json
{
	public static class DynamicObjectExtensions
	{
		public static string Days(this DateTime self)
		{
			return DateTools.DateToString(self, DateTools.Resolution.DAY);
		}

		public static string Hours(this DateTime self)
		{
			return DateTools.DateToString(self, DateTools.Resolution.HOUR);
		}

		public static string Minutes(this DateTime self)
		{
			return DateTools.DateToString(self, DateTools.Resolution.MINUTE);
		}

		public static string Secoonds(this DateTime self)
		{
			return DateTools.DateToString(self, DateTools.Resolution.SECOND);
		}
	}
}
