using System;

namespace Raven.Database.Storage
{
	internal static class StringHelper
	{
		public static bool Compare(string startsWith, string docId, bool exactMatch)
		{
			if (string.IsNullOrEmpty(startsWith))
				return true;

			if (exactMatch)
			{
				if (docId.Equals(startsWith, StringComparison.OrdinalIgnoreCase))
					return true;
			}
			else if (docId.StartsWith(startsWith, StringComparison.OrdinalIgnoreCase))
			{
				return true;
			}

			return false;
		}
	}
}
