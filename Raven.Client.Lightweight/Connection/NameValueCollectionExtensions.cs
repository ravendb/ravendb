// -----------------------------------------------------------------------
//  <copyright file="NameValueCollectionExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Specialized;

namespace Raven.Client.Connection
{
	public static class NameValueCollectionExtensions
	{
		public static int? GetAsInt(this NameValueCollection nameValueCollection, string name)
		{
			var value = nameValueCollection[name];
			if (value == null)
				return null;

			int number;
			if (int.TryParse(value, out number))
			{
				return number;
			}

			return null;
		}
	}
}
