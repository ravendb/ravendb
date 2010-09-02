using System;
using System.Globalization;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ComparingGuids
	{
		[Fact]
		public void CanCompareGuidsAsGuidAndBinary()
		{
			var docEtagBinary = ToByteArray("00000001000000001c080d18cd08312d");
			var indexEtagBinary = ToByteArray("000001c3000000003edb0118cd08f6b4");
			
			var docEtag = docEtagBinary.TransfromToGuidWithProperSorting();
			var indexEtag = indexEtagBinary.TransfromToGuidWithProperSorting();
			Assert.Equal(CompareArrays(docEtagBinary, indexEtagBinary),
				docEtag.CompareTo(indexEtag));
		}
		
		[Fact]
		public void CanRoundTripGuids()
		{
			for (int i = 0; i < 500; i++)
			{
				var newGuid = Guid.NewGuid();
				Assert.Equal(newGuid, newGuid.TransformToValueForEsentSorting().TransfromToGuidWithProperSorting());
			}
		}

		private static byte[] ToByteArray(string data)
		{
			var bytes = new byte[16];
			int j = 0;
			for (int i = 0; i < 32; i += 2)
			{
				var substring = data.Substring(i, 2);
				bytes[j++] = byte.Parse(substring, NumberStyles.HexNumber);
			}
			return bytes;
		}

		private static int CompareArrays(byte[] docEtagBinary, byte[] indexEtagBinary)
		{
			for (int i = 0; i < docEtagBinary.Length; i++)
			{
				if (docEtagBinary[i].CompareTo(indexEtagBinary[i]) != 0)
				{
					return docEtagBinary[i].CompareTo(indexEtagBinary[i]);
				}
			}
			return 0;
		}
	}
}