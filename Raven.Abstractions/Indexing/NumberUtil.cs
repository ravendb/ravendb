//-----------------------------------------------------------------------
// <copyright file="NumberUtil.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;

namespace Raven.Abstractions.Indexing
{
	/// <summary>
	/// Helper function for numeric to indexed string and vice versa
	/// </summary>
	public class NumberUtil
	{
		/// <summary>
		/// Translate a number to an indexable string
		/// </summary>
		public static string NumberToString(int number)
		{
			return "Ix" + number.ToString("G", CultureInfo.InvariantCulture);
		}

		/// <summary>
		/// Translate a number to an indexable string
		/// </summary>
		public static string NumberToString(long number)
		{
			return "Lx" + number.ToString("G", CultureInfo.InvariantCulture);
		}

		/// <summary>
		/// Translate a number to an indexable string
		/// </summary>
		public static string NumberToString(float number)
		{
			return "Fx" + number.ToString("G", CultureInfo.InvariantCulture);
		}

		/// <summary>
		/// Translate a number to an indexable string
		/// </summary>
		public static string NumberToString(double number)
		{
			return "Dx" + number.ToString("G",CultureInfo.InvariantCulture);
		}

		/// <summary>
		/// Translate an indexable string to a number
		/// </summary>
		public static object StringToNumber(string number)
		{
			if (number == null)
				return null;

			if ("NULL".Equals(number, StringComparison.InvariantCultureIgnoreCase) || 
				"*".Equals(number,StringComparison.InvariantCultureIgnoreCase))
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
				case "Ix":
					return int.Parse(num, CultureInfo.InvariantCulture);
				case "Lx":
					return long.Parse(num, CultureInfo.InvariantCulture);
				case "Fx":
					return  float.Parse(num, CultureInfo.InvariantCulture);
				case "Dx":
					return  double.Parse(num, CultureInfo.InvariantCulture);
			}

			throw new ArgumentException(string.Format("Could not understand how to parse: '{0}'", number));

		}
	}
}
