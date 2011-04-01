namespace Raven.Studio.Framework.Extensions
{
	using System.Collections.Generic;

	public static class ListExtensions
	{
		public static void Add(this IList<string> list, string format, params object[] args)
		{
			list.Add(string.Format(format, args));
		}
	}
}