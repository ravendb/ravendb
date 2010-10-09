using System.IO;
using Raven.Storage.Managed.Data;
using Xunit;

namespace Raven.Storage.Tests
{
	public class EncodingUsing7Bits
	{
		[Fact]
		public void CanEncodeUsing7BitInt64()
		{
			var buffer = new byte[1024];

			for (long i = 0; i < 64*1024; i++)
			{
				var bw = new BinaryWriterWith7BitEncoding(new MemoryStream(buffer));
				bw.Write7BitEncodedInt64(i);
				var br = new BinaryReaderWith7BitEncoding(new MemoryStream(buffer));
				Assert.Equal(i, br.Read7BitEncodedInt64());
			}
		}

		[Fact]
		public void CanEncodeUsing7BitInt64WithNullableInt()
		{
			var buffer = new byte[1024];

			for (long i = 0; i < 64 * 1024; i++)
			{
				var bw = new BinaryWriterWith7BitEncoding(new MemoryStream(buffer));
				bw.WriteBitEncodedNullableInt64(i);
				var br = new BinaryReaderWith7BitEncoding(new MemoryStream(buffer));
				Assert.Equal(i, br.ReadBitEncodedNullableInt64());
			}
		}
	}
}