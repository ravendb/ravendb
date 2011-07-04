// -----------------------------------------------------------------------
//  <copyright file="CanGetStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Studio.Common
{
	public static class StringExtensions
	{
		public static string FirstWord(this string text)
		{
			if (text == null)
				return null;

			var index = text.IndexOf(' ');
			if (index == -1)
				return text;
			
			return text.Substring(0, index);
		}
	}
}