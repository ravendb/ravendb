using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Voron.Util;

namespace Voron
{
	public unsafe class StructureReader<T>
	{
		private readonly Structure<T> _value;
		private readonly byte* _ptr;
		private readonly StructureSchema<T> _schema;
		public StructureReader(byte* ptr, StructureSchema<T> schema)
		{
			_ptr = ptr;
			_schema = schema;
		}

		public StructureReader(Structure<T> value, StructureSchema<T> schema)
		{
			_value = value;
			_schema = schema;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private int FixedOffset(T field)
		{
			return ((FixedSizeField) _schema.Fields[field.GetHashCode()]).Offset;
		}

		public uint* VariableOffsets
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get { return (uint*) (_ptr + _schema.FixedSize); }
		}

		private int Read7BitEncodedInt(byte* ptr, out int size)
		{
			size = 0;
			// Read out an Int32 7 bits at a time.  The high bit 
			// of the byte when on means to continue reading more bytes.
			int value = 0;
			int shift = 0;
			byte b;
			do
			{
				// Check for a corrupted stream.  Read a max of 5 bytes. 
				if (shift == 5 * 7)  // 5 bytes max per Int32, shift += 7
					throw new InvalidDataException("Invalid 7bit shifted value, used more than 5 bytes");

				// ReadByte handles end of stream cases for us.
				b = *ptr;
				ptr++;
				size++;
				value |= (b & 0x7F) << shift;
				shift += 7;
			} while ((b & 0x80) != 0);

			return value;
		}

		public int ReadInt(T field)
		{
			if(_ptr != null)
				return *((int*)(_ptr + FixedOffset(field)));

			return (int) _value.FixedSizeWrites[field].Value;
		}

		public uint ReadUInt(T field)
		{
			if (_ptr != null)
				return *((uint*) (_ptr + FixedOffset(field)));

			return (uint) _value.FixedSizeWrites[field].Value;
		}

		public long ReadLong(T field)
		{
			if (_ptr != null)
				return *((long*)(_ptr + FixedOffset(field)));

			return (long) _value.FixedSizeWrites[field].Value;
		}

		public ulong ReadULong(T field)
		{
			if (_ptr != null)
				return *((ulong*) (_ptr + FixedOffset(field)));

			return (ulong) _value.FixedSizeWrites[field].Value;
		}

		public char ReadChar(T field)
		{
			if (_ptr != null)
				return *((char*) (_ptr + FixedOffset(field)));

			return (char) _value.FixedSizeWrites[field].Value;
		}

		public byte ReadByte(T field)
		{
			if (_ptr != null)
				return *(_ptr + FixedOffset(field));

			return (byte) _value.FixedSizeWrites[field].Value;
		}

		public bool ReadBool(T field)
		{
			byte value;
			if (_ptr != null)
			{
				value = (*(_ptr + FixedOffset(field)));
			}
			else
			{
				value = (byte) _value.FixedSizeWrites[field].Value;
			}

			switch (value)
			{
				case 0:
					return false;
				case 1:
					return true;
				default:
					throw new InvalidDataException("Unexpected boolean value: " + value);
			}
		}

		public sbyte ReadSByte(T field)
		{
			if (_ptr != null)
				return *((sbyte*) (_ptr + FixedOffset(field)));

			return (sbyte) _value.FixedSizeWrites[field].Value;
		}

		public short ReadShort(T field)
		{
			if (_ptr != null)
				return *((short*) (_ptr + FixedOffset(field)));

			return (short) _value.FixedSizeWrites[field].Value;
		}

		public ushort ReadUShort(T field)
		{
			if (_ptr != null)
				return *((ushort*) (_ptr + FixedOffset(field)));

			return (ushort) _value.FixedSizeWrites[field].Value;
		}

		public float ReadFloat(T field)
		{
			if (_ptr != null)
				return *((float*) (_ptr + FixedOffset(field)));

			return (float) _value.FixedSizeWrites[field].Value;
		}

		public double ReadDouble(T field)
		{
			if (_ptr != null)
				return *((double*) (_ptr + FixedOffset(field)));

			return (double) _value.FixedSizeWrites[field].Value;
		}

		public decimal ReadDecimal(T field)
		{
			if (_ptr != null)
				return *((decimal*) (_ptr + FixedOffset(field)));

			return (decimal) _value.FixedSizeWrites[field].Value;
		}

		public string ReadString(T field)
		{
			var fieldIndex = ((VariableSizeField)_schema.Fields[field.GetHashCode()]).Index;

			if (_ptr != null)
			{
				var offset = VariableOffsets[fieldIndex];
				int valueLengthSize;
				var length = Read7BitEncodedInt(_ptr + offset, out valueLengthSize);

				return new string((sbyte*)(_ptr + offset + valueLengthSize), 0, length, Encoding.UTF8);
			}

			return _value.VariableSizeWrites[fieldIndex].ValueString;
		}

		public byte[] ReadBytes(T field)
		{
			var fieldIndex = ((VariableSizeField)_schema.Fields[field.GetHashCode()]).Index;

			if (_ptr != null)
			{
				var offset = VariableOffsets[fieldIndex];
				int valueLengthSize;
				var length = Read7BitEncodedInt(_ptr + offset, out valueLengthSize);

				var result = new byte[length];

				fixed (byte* rPtr = result)
				{
					MemoryUtils.Copy(rPtr, _ptr + offset + valueLengthSize, length);
				}

				return result;
			}

			return _value.VariableSizeWrites[fieldIndex].Value;
		}
	}
}