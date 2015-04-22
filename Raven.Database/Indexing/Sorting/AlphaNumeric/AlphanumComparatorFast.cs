using System;
using System.Runtime.CompilerServices;
using System.Text;

namespace Raven.Database.Indexing.Sorting.AlphaNumeric
{
	public static class AlphanumComparatorFast
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static bool IsDigitOrDecimal(char c)
		{
			return Char.IsDigit(c) || c == '.';
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static int Compare(string s1, string s2)
		{
			int len1 = s1.Length;
			int len2 = s2.Length;
			int marker1 = 0;
			int marker2 = 0;

			// Walk through two the strings with two markers.
			while (marker1 < len1 && marker2 < len2)
			{
				char ch1 = s1[marker1];
				char ch2 = s2[marker2];

				// Some buffers we can build up characters in for each chunk.
				char[] space1 = new char[len1 - marker1];
				int loc1 = 0;
				char[] space2 = new char[len2 - marker2];
				int loc2 = 0;

				// Walk through all following characters that are digits or
				// characters in BOTH strings starting at the appropriate marker.
				// Collect char arrays.
				do
				{
					space1[loc1++] = ch1;
					marker1++;

					if (marker1 < len1)
					{
						ch1 = s1[marker1];
					}
					else
					{
						break;
					}
				} while (IsDigitOrDecimal(ch1) == IsDigitOrDecimal(space1[0]));
				if (space1[--loc1] == '.')
				{
					space1[loc1] = '\0';
					marker1--;
				}

				do
				{
					space2[loc2++] = ch2;
					marker2++;

					if (marker2 < len2)
					{
						ch2 = s2[marker2];
					}
					else
					{
						break;
					}
				} while (IsDigitOrDecimal(ch2) == IsDigitOrDecimal(space2[0]));
				if (space2[--loc2] == '.')
				{
					space2[loc2] = '\0';
					marker2--;
				}


				//space1 - string
				//space2 - string
				decimal decimal1;
				//var isDecimal1 = decimal.TryParse(space1, out decimal1);

				// If we have collected numbers, compare them numerically.
				// Otherwise, if we have strings, compare them alphabetically.
				string trimmedStr1 = GetTrimmedString(space1);//new string(space1).Trim('\0');
				string trimmedStr2 = GetTrimmedString(space2);

				int result;
				if (Char.IsDigit(space1[0]) && Char.IsDigit(space2[0]))
				{
					long thisNumericChunk = long.Parse(trimmedStr1);
					long thatNumericChunk = long.Parse(trimmedStr2);
					result = thisNumericChunk.CompareTo(thatNumericChunk);
				}
				else
				{
					result = String.CompareOrdinal(trimmedStr1, trimmedStr2);
				}

				if (result != 0)
				{
					return result;
				}
			}
			return len1 - len2;
		}

		private const char NullCharValue = '\0';

		private static string GetTrimmedString(char[] buffer)
		{
			var nullIndex = Array.IndexOf(buffer, NullCharValue);
			nullIndex = nullIndex >= 0 ? nullIndex : buffer.Length;
			var str = new string(buffer, 0, nullIndex);
			return str;
		}
	}
}