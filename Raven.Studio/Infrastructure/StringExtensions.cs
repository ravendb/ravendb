namespace Raven.Studio.Infrastructure
{
	public static class StringExtensions
	{
		public static string ReplaceSingle(this string str, string toReplace, string newString)
		{
			var index = str.IndexOf(toReplace);
			if (index == -1)
				return newString;

			return str.Remove(index, toReplace.Length).Insert(index, newString);
		}
	}
}