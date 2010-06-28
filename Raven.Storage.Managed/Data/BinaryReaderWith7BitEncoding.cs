using System;
using System.IO;

namespace Raven.Storage.Managed.Data
{
	public class BinaryReaderWith7BitEncoding : BinaryReader
	{
		public BinaryReaderWith7BitEncoding(Stream output)
			: base(output)
		{
		}

		public new int Read7BitEncodedInt()
		{
			return base.Read7BitEncodedInt();
		}

		public long? ReadBitEncodedNullableInt64()
		{
			byte b = ReadByte();
			if (b == 1)
				return null;
			return Read7BitEncodedInt64();
		}

		public long Read7BitEncodedInt64()
		{
			byte b;
			long num = 0;
			int num2 = 0;
			do
			{
				if (num2 >= 63)
				{
					throw new FormatException("Invalid bit long");
				}
				b = ReadByte();
				num |= ((long) (b & 0x7f) << num2);
				num2 += 7;
			} while ((b & 0x80) != 0);
			return num;
		}
	}
}