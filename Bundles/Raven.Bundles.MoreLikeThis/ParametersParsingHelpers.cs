namespace Raven.Bundles.MoreLikeThis
{
	public static class ParametersParsingHelpers
	{
		public static int? ToNullableInt(this string value)
		{
			int ret;
			if (value == null || !int.TryParse(value, out ret)) return null;
			return ret;
		}

		public static bool? ToNullableBool(this string value)
		{
			bool ret;
			if (value == null || ! bool.TryParse(value, out ret)) return null;
			return ret;
		}
	}
}
