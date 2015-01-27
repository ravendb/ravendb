using System;

namespace Voron
{
	public unsafe class StructureReader<T>
	{
		private readonly Structure<T> value;
		private readonly byte* ptr;
		private readonly StructureSchema<T> schema;

		public StructureReader(byte* ptr, StructureSchema<T> schema)
		{
			this.ptr = ptr;
			this.schema = schema;
		}

		public StructureReader(Structure<T> value, StructureSchema<T> schema)
		{
			this.value = value;
			this.schema = schema;
		}

		private int FixedFieldOffset(T field)
		{
			return schema._fixedSizeFields[field].Offset;
		}

		private int VariableFieldSize(T field)
		{
			return *((int*) (ptr + schema.FixedSize + sizeof (int)*schema._variableSizeFields[field].Index));
		}

		public int ReadInt(T field)
		{
			if(ptr != null)
				return *((int*)(ptr + FixedFieldOffset(field)));

			return (int) value._fixedSizeWrites[field].Value;
		}

		public long ReadLong(T field)
		{
			if (ptr != null)
				return *((long*)(ptr + FixedFieldOffset(field)));

			return (long) value._fixedSizeWrites[field].Value;
		}

		public byte ReadByte(T field)
		{
			if (ptr != null)
				return *(ptr + FixedFieldOffset(field));

			return (byte) value._fixedSizeWrites[field].Value;
		}

		public string ReadString(T field)
		{
			if (ptr != null)
			{
				var size = VariableFieldSize(field);

				//var length
			}

			throw new NotImplementedException();
		}
	}
}