using System;

namespace Raven.Database.Extensions
{
	public static class GuidExtensions
	{
		public static Guid TransfromToGuidWithProperSorting(this byte[] bytes)
		{
			var unnormalized = new byte[16];
			unnormalized[10] = bytes[0];
			unnormalized[11] = bytes[1];
			unnormalized[12] = bytes[2];
			unnormalized[13] = bytes[3];
			unnormalized[14] = bytes[4];
			unnormalized[15] = bytes[5];
			unnormalized[8] = bytes[6];
			unnormalized[9] = bytes[7];
			unnormalized[6] = bytes[8];
			unnormalized[7] = bytes[9];
			unnormalized[4] = bytes[10];
			unnormalized[5] = bytes[11];
			unnormalized[0] = bytes[12];
			unnormalized[1] = bytes[13];
			unnormalized[2] = bytes[14];
			unnormalized[3] = bytes[15];
			return new Guid(unnormalized);
		}

		public static byte[] TransformToValueForEsentSorting(this Guid guid)
		{
			var bytes = guid.ToByteArray();
			var normalized = new byte[16];
			normalized[0] = bytes[10];
			normalized[1] = bytes[11];
			normalized[2] = bytes[12];
			normalized[3] = bytes[13];
			normalized[4] = bytes[14];
			normalized[5] = bytes[15];
			normalized[6] = bytes[8];
			normalized[7] = bytes[9];
			normalized[8] = bytes[6];
			normalized[9] = bytes[7];
			normalized[10] = bytes[4];
			normalized[11] = bytes[5];
			normalized[12] = bytes[0];
			normalized[13] = bytes[1];
			normalized[14] = bytes[2];
			normalized[15] = bytes[3];
			return normalized;
		}
	}
}