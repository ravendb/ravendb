namespace Ionic.Zlib
{
	/// <summary>
	/// Computes an Adler-32 checksum. 
	/// </summary>
	/// <remarks>
	/// The Adler checksum is similar to a CRC checksum, but faster to compute, though less
	/// reliable.  It is used in producing RFC1950 compressed streams.  The Adler checksum
	/// is a required part of the "ZLIB" standard.  Applications will almost never need to
	/// use this class directly.
	/// </remarks>
	internal sealed class Adler
	{
		// largest prime smaller than 65536
		private static readonly int BASE = 65521;
		// NMAX is the largest n such that 255n(n+1)/2 + (n+1)(BASE-1) <= 2^32-1
		private static readonly int NMAX = 5552;

		static internal uint Adler32(uint adler, byte[] buf, int index, int len)
		{
			if (buf == null)
				return 1;

			int s1 = (int) (adler & 0xffff);
			int s2 = (int) ((adler >> 16) & 0xffff);

			while (len > 0)
			{
				int k = len < NMAX ? len : NMAX;
				len -= k;
				while (k >= 16)
				{
					//s1 += (buf[index++] & 0xff); s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					s1 += buf[index++]; s2 += s1;
					k -= 16;
				}
				if (k != 0)
				{
					do
					{
						s1 += buf[index++]; 
						s2 += s1;
					}
					while (--k != 0);
				}
				s1 %= BASE;
				s2 %= BASE;
			}
			return (uint)((s2 << 16) | s1);
		}

	}
}