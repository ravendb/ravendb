using System.IO;

namespace Raven.Storage.Managed.Data
{
	public class BinaryWriterWith7BitEncoding : BinaryWriter
	{
		public BinaryWriterWith7BitEncoding(Stream output) : base(output)
		{
		}

		public new void Write7BitEncodedInt(int i)
		{
			base.Write7BitEncodedInt(i);
		}

		public void WriteBitEncodedNullableInt64(long? value)
		{
			if (value == null)
			{
				Write((byte)1);
				return;
			}
			Write((byte) 0);
			Write7BitEncodedInt64(value.Value);
		}

		public void Write7BitEncodedInt64(long value)
		{
			var num = (ulong)value;
			while (num >= 0x80)
			{
				Write((byte)(num | 0x80));
				num = num >> 7;
			}
			Write((byte)num);
		}
	}
}