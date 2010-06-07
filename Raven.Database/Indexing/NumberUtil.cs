using System;
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

		public static string NumberToString(float number)
		{
			return "Fx" + number.ToString("F", CultureInfo.InvariantCulture);
		}

		public static string NumberToString(double number)
		{
			return "Dx" + number.ToString("F",CultureInfo.InvariantCulture);
		}

		public static object StringToNumber(string number)
		{
			if ("NULL".Equals(number, StringComparison.InvariantCultureIgnoreCase))
				return null;
			if(number.Length <= 2)
				throw new ArgumentException("String must be greater than 2 characters");
			var num = number.Substring(2);
			var prefix = number.Substring(0, 2);
			switch (prefix)
			{
				case "0x":
					switch (num.Length)
					{
						case 8:
							return int.Parse(num, NumberStyles.HexNumber);
						case 16:
							return long.Parse(num, NumberStyles.HexNumber);
					}
					break;
				case "Fx":
					return  float.Parse(num, CultureInfo.InvariantCulture);
				case "Dx":
					return  double.Parse(num, CultureInfo.InvariantCulture);
			}

			throw new ArgumentException("Could not understand how to parse: " + number);

		}
	}
}