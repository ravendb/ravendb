using System.Globalization;

namespace Raven.Database.Indexing
{
	public class NumberUtil
	{
		public static string NumberToString(int number)
		{
			return string.Format("0x{0:X8}", number);
		}

		public static string NumberToString(long number)
		{
			return string.Format("0x{0:X16}", number);
		}

		public static string NumberToString(decimal number)
		{
			return number.ToString(CultureInfo.InvariantCulture);
		}

		public static string NumberToString(float number)
		{
			return number.ToString(CultureInfo.InvariantCulture);
		}

		public static string NumberToString(double number)
		{
			return number.ToString(CultureInfo.InvariantCulture);
		}
	}
}