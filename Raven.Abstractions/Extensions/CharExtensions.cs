namespace Raven.Abstractions.Extensions
{
	public static class CharExtensions
	{
		public static string CharToString(this char c)
		{
			return c.ToString(System.Globalization.CultureInfo.InvariantCulture);
		}
	}
}