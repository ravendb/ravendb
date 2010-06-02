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

		public static string NumberToString(decimal number)
		{
			return "Mx" + number.ToString(CultureInfo.InvariantCulture);
		}

		public static string NumberToString(float number)
		{
			return "Fx" + number.ToString(CultureInfo.InvariantCulture);
		}

		public static string NumberToString(double number)
		{
			return "Dx" + number.ToString(CultureInfo.InvariantCulture);
		}

		public static object StringToNumber(string number)
		{
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
							var i = int.Parse(num, NumberStyles.HexNumber);
							return i == Int32.MaxValue || i == Int32.MinValue? null : (object)i;
						case 16:
							var l = long.Parse(num, NumberStyles.HexNumber);
							return l == Int64.MaxValue || l == Int64.MinValue ? null : (object) l;
					}
					break;
				case "Mx":
					var dec = decimal.Parse(num, CultureInfo.InvariantCulture);
					return dec == Decimal.MaxValue || dec == Decimal.MinValue ? null : (object) dec;
				case "Fx":
					var f = float.Parse(num, CultureInfo.InvariantCulture);
					return f == Single.MaxValue || f == Double.MinValue ? null : (object) f;
				case "Dx":
					var d = double.Parse(num, CultureInfo.InvariantCulture);
					return d == Double.MaxValue || d == Double.MinValue ? null : (object) d;
			}

			throw new ArgumentException("Could not understand how to parse: " + number);

		}
	}
}