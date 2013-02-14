// -----------------------------------------------------------------------
//  <copyright file="StringExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Client.WinRT.MissingFromWinRT
{
	public static class StringExtensions
	{
		public static bool Contains(this string str, char c)
		{
			return str.Contains(c.ToString());
		}
	}
}