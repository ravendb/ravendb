namespace Raven.Abstractions.Extensions
{
	public static class CharExtensions
	{
		public static string CharToString(this char c)
		{
#if NETFX_CORE
			return c.ToString();
#else
			return c.ToString(System.Globalization.CultureInfo.InvariantCulture);
#endif
		}
	}
}