// -----------------------------------------------------------------------
//  <copyright file="IStructure.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Voron.Util;

namespace Voron
{
	public unsafe interface IStructure
	{
		void Write(byte* ptr);

		int GetSize();

		void AssertValidStructure();
	}

	public unsafe class Structure<T> : IStructure
	{
		internal class FixedSizeWrite
		{
			public ValueType Value;
			public FixedSizeField FieldInfo;
		}

		internal class IncrementWrite
		{
			public FixedSizeField FieldInfo;
			public long IncrementValue;
		}

		internal class VariableSizeWrite
		{
			public byte[] Value;
            public string ValueString;

            public int ValueSize;
			public byte ValueSizeLength;

			public int Index;
		}

		private const int VariableFieldOffsetSize = sizeof(uint);
		private readonly StructureSchema<T> _schema;
		internal readonly Dictionary<T, FixedSizeWrite> FixedSizeWrites = new Dictionary<T, FixedSizeWrite>();
		internal readonly Dictionary<T, IncrementWrite> IncrementWrites = new Dictionary<T, IncrementWrite>();
		internal VariableSizeWrite[] VariableSizeWrites = null;

		public Structure(StructureSchema<T> schema)
		{
			_schema = schema;
		}

		public bool AllowToSkipVariableSizeFields { get; set; }

		public Structure<T> Set<TValue>(T field, TValue value)
		{
			StructureField structureField;
			try
			{
				structureField = _schema.Fields[(int) (object) field];
			}
			catch (IndexOutOfRangeException)
			{
				throw new ArgumentException("No such field in schema defined. Field name: " + field);
			}

			var type = typeof(TValue);

			var fixedSizeField = structureField as FixedSizeField;
			var variableSizeField = structureField as VariableSizeField;

			if (fixedSizeField != null)
			{
				if (type != fixedSizeField.Type)
					throw new InvalidDataException(string.Format("Attempt to set a field value which type is different than defined in the structure schema. Expected: {0}, got: {1}", fixedSizeField.Type, type));

				var valueTypeValue = value as ValueType;

				if (valueTypeValue == null)
					throw new NotSupportedException("Unexpected fixed size value type: " + type);

				FixedSizeWrites.Add(field, new FixedSizeWrite
				{
					Value = valueTypeValue, FieldInfo = fixedSizeField
				});
			}
			else if (variableSizeField != null)
			{
				if (type != variableSizeField.Type)
					throw new InvalidDataException(string.Format("Attempt to set a field value which type is different than defined in the structure schema. Expected: {0}, got: {1}", variableSizeField.Type, type));

                if (VariableSizeWrites == null)
                    VariableSizeWrites = new VariableSizeWrite[_schema.VariableFieldsCount];

                var variableSizeWrite = new VariableSizeWrite
                {
                    ValueString = value as string,
                    Value = value as byte[],                    
                };

                if ( variableSizeWrite.ValueString == null && variableSizeWrite.Value == null )
                    throw new NotSupportedException("Unexpected variable size value type: " + type);

                if (variableSizeWrite.Value != null )
                {
                    variableSizeWrite.ValueSize = variableSizeWrite.Value.Length;                    
                }
                else
                {
                    variableSizeWrite.ValueSize = Encoding.UTF8.GetByteCount(variableSizeWrite.ValueString);
                }
                variableSizeWrite.ValueSizeLength = SizeOf7BitEncodedInt(variableSizeWrite.ValueSize);


                VariableSizeWrites[variableSizeField.Index] = variableSizeWrite;              
			}
			else
				throw new NotSupportedException("Unexpected structure field type: " + type);

			return this;
		}

		public Structure<T> Increment(T field, long delta)
		{
			FixedSizeField fixedSizeField;
			try
			{
				fixedSizeField = (FixedSizeField) _schema.Fields[(int) (object) field];
			}
			catch (IndexOutOfRangeException)
			{
				throw new ArgumentException("No such field in schema defined. Field name: " + field);
			}

			IncrementWrites.Add(field, new IncrementWrite { IncrementValue = delta, FieldInfo = fixedSizeField });

			return this;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void AssertValidStructure()
		{
			if (_schema.IsFixedSize == false && VariableSizeWrites == null && AllowToSkipVariableSizeFields == false)
				throw new InvalidOperationException("Your structure schema defines variable size fields but you haven't set any. If you really want to skip those fields set AllowToSkipVariableSizeFields = true.");

			if (_schema.IsFixedSize == false && VariableSizeWrites != null && VariableSizeWrites.Any(x => x == null))
			{
				var missingFields = new List<object>();

				for (int i = 0; i < VariableSizeWrites.Length; i++)
				{
					if (VariableSizeWrites[i] != null)
						continue;

					missingFields.Add(_schema.Fields.OfType<VariableSizeField>().First(x => x.Index == i).Name);
				}

				throw new InvalidOperationException("Your structure has variable size fields. You have to set all of them to properly write a structure and avoid overlapping fields. Missing fields: " + string.Join(", ", missingFields));
			}
		}

		public void Write(byte* ptr)
		{
			WriteFixedSizeFields(ptr);
			WriteIncrements(ptr);
			WriteVariableSizeFields(ptr);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private void WriteFixedSizeFields(byte* ptr)
		{
			if (FixedSizeWrites.Count == 0)
				return;

			foreach (var fixedSizeWrite in FixedSizeWrites.Values)
			{
				var fieldInfo = fixedSizeWrite.FieldInfo;

				if (fieldInfo.Type == typeof(int))
				{
					*((int*) (ptr + fieldInfo.Offset)) = (int) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(long))
				{
					*((long*) (ptr + fieldInfo.Offset)) = (long) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(byte))
				{
					*(ptr + fieldInfo.Offset) = (byte) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(float))
				{
					*((float*) (ptr + fieldInfo.Offset)) = (float) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(double))
				{
					*((double*) (ptr + fieldInfo.Offset)) = (double) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(decimal))
				{
					*((decimal*) (ptr + fieldInfo.Offset)) = (decimal) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(short))
				{
					*((short*) (ptr + fieldInfo.Offset)) = (short) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(bool))
				{
					var booleanValue = (bool) fixedSizeWrite.Value;
					*(ptr + fieldInfo.Offset) = booleanValue ? (byte) 1 : (byte) 0;
				}
				else if (fieldInfo.Type == typeof(char))
				{
					*((char*) (ptr + fieldInfo.Offset)) = (char) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(uint))
				{
					*((uint*) (ptr + fieldInfo.Offset)) = (uint) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(ulong))
				{
					*((ulong*) (ptr + fieldInfo.Offset)) = (ulong) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(sbyte))
				{
					*((sbyte*) (ptr + fieldInfo.Offset)) = (sbyte) fixedSizeWrite.Value;
				}
				else if (fieldInfo.Type == typeof(ushort))
				{
					*((ushort*) (ptr + fieldInfo.Offset)) = (ushort) fixedSizeWrite.Value;
				}
				else
				{
					throw new NotSupportedException("Unexpected fixed size type: " + fieldInfo.Type);
				}
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void WriteIncrements(byte* ptr)
		{
			if (IncrementWrites.Count == 0)
				return;

			foreach (var incrementWrite in IncrementWrites.Values)
			{
				var fieldInfo = incrementWrite.FieldInfo;

				if (fieldInfo.Type == typeof(int))
				{
					*((int*) (ptr + fieldInfo.Offset)) += (int) incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(long))
				{
					*((long*) (ptr + fieldInfo.Offset)) += incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(byte))
				{
					*(ptr + fieldInfo.Offset) += (byte) incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(float))
				{
					*((float*) (ptr + fieldInfo.Offset)) += incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(double))
				{
					*((double*) (ptr + fieldInfo.Offset)) += incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(decimal))
				{
					*((decimal*) (ptr + fieldInfo.Offset)) += incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(short))
				{
					*((short*) (ptr + fieldInfo.Offset)) += (short) incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(bool))
				{
					throw new InvalidOperationException("Cannot increment boolean field");
				}
				else if (fieldInfo.Type == typeof(char))
				{
					*((char*) (ptr + fieldInfo.Offset)) += (char) incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(uint))
				{
					*((uint*) (ptr + fieldInfo.Offset)) += (uint) incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(ulong))
				{
					*((ulong*) (ptr + fieldInfo.Offset)) += (ulong) incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(sbyte))
				{
					*((sbyte*) (ptr + fieldInfo.Offset)) += (sbyte) incrementWrite.IncrementValue;
				}
				else if (fieldInfo.Type == typeof(ushort))
				{
					*((ushort*) (ptr + fieldInfo.Offset)) += (ushort) incrementWrite.IncrementValue;
				}
				else
				{
					throw new NotSupportedException("Unexpected fixed size type: " + fieldInfo.Type);
				}
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private void WriteVariableSizeFields(byte* ptr)
		{
			if (VariableSizeWrites == null)
				return;

			var fieldOffsetsSize = VariableFieldOffsetSize * VariableSizeWrites.Length;

			var offsetsPointer = ptr + _schema.FixedSize;
			var fieldPointer = offsetsPointer + fieldOffsetsSize;

			var offsets = new uint[VariableSizeWrites.Length];

			for (int i = 0; i < VariableSizeWrites.Length; i++)
			{
				var write = VariableSizeWrites[i];

                int valueLength = write.ValueSize;

                offsets[i] = (uint)(fieldPointer - ptr);                

                Write7BitEncodedInt(fieldPointer, valueLength);
                fieldPointer += write.ValueSizeLength;

                if ( write.Value != null ) // We have an array of bytes
                {
                    fixed (byte* valuePtr = write.Value)
                    {
                        MemoryUtils.Copy(fieldPointer, valuePtr, valueLength);
                    }
                }
                else // We have an string
                {
                    fixed (char* valuePtr = write.ValueString)
                    {
                        Encoding.UTF8.GetBytes(valuePtr, write.ValueString.Length, fieldPointer, valueLength);
                    }                        
                }

                fieldPointer += valueLength;
			}

			fixed (uint* p = offsets)
			{
				MemoryUtils.Copy(offsetsPointer, (byte*) p, fieldOffsetsSize);
			}
		}

		public int GetSize()
		{
			if (_schema.IsFixedSize)
				return _schema.FixedSize;

			return _schema.FixedSize + // fixed size fields 
				   (VariableSizeWrites == null ? 0 : VariableFieldOffsetSize * VariableSizeWrites.Length + // offsets of variable size fields 
					   VariableSizeWrites.Sum(x => x.ValueSize + x.ValueSizeLength)); // variable size fields
		}

		private static byte SizeOf7BitEncodedInt(int value)
		{
			byte size = 1;
			var v = (uint) value;
			while (v >= 0x80)
			{
				size++;
				v >>= 7;
			}

			return size;
		}
		private static void Write7BitEncodedInt(byte* ptr, int value)
		{
			// Write out an int 7 bits at a time.  The high bit of the byte, 
			// when on, tells reader to continue reading more bytes. 
			var v = (uint) value;   // support negative numbers
			while (v >= 0x80)
			{
				*ptr = (byte) (v | 0x80);
				ptr++;
				v >>= 7;
			}
			*ptr = (byte) (v);
		}
	}
}