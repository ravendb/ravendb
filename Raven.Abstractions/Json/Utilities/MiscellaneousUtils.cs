using System;
using System.Globalization;

namespace Raven.Json.Utilities
{
	internal static class MiscellaneousUtils
	{

		public static ArgumentOutOfRangeException CreateArgumentOutOfRangeException(string paramName, object actualValue, string message)
		{
			string newMessage = message + Environment.NewLine + @"Actual value was {0}.".FormatWith(CultureInfo.InvariantCulture, actualValue);

			return new ArgumentOutOfRangeException(paramName, newMessage);
		}

		public static int ByteArrayCompare(byte[] a1, byte[] a2)
		{
			int lengthCompare = a1.Length.CompareTo(a2.Length);
			if (lengthCompare != 0)
				return lengthCompare;

			for (int i = 0; i < a1.Length; i++)
			{
				int valueCompare = a1[i].CompareTo(a2[i]);
				if (valueCompare != 0)
					return valueCompare;
			}

			return 0;
		}
	}
}
