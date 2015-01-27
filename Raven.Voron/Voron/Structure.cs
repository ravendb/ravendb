// -----------------------------------------------------------------------
//  <copyright file="Structure.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Voron.Impl;
using Voron.Util;

namespace Voron
{
	public unsafe abstract class Structure
	{
		public abstract void Write(byte* ptr);

		public abstract int GetSize();
	}

	public unsafe class Structure<T> : Structure
	{
		internal class FixedSizeWrite
		{
			public ValueType Value;
			public FixedSizeField FieldInfo;
		}

		private readonly StructureSchema<T> _schema;
		internal readonly Dictionary<T, FixedSizeWrite> _fixedSizeWrites = new Dictionary<T, FixedSizeWrite>();
		internal readonly Dictionary<T, string> _variableSizeWrites = new Dictionary<T, string>(); 

		public Structure(StructureSchema<T> schema)
		{
			_schema = schema;
		}

		public Structure<T> Set<TValue>(T field, TValue value)
		{
			FixedSizeField fixedSizeField;
			VariableSizeField variableSizeField = null;

			if (_schema._fixedSizeFields.TryGetValue(field, out fixedSizeField) == false && _schema._variableSizeFields.TryGetValue(field, out variableSizeField) == false)
				throw new InvalidOperationException("No such field in schema defined. Field name: " + field);

			if (fixedSizeField != null)
			{
				var valueTypeValue = value as ValueType;

				if (valueTypeValue == null)
					throw new NotSupportedException("Unexpected fixed size value type: " + value.GetType());

				_fixedSizeWrites.Add(field, new FixedSizeWrite { Value = valueTypeValue, FieldInfo = fixedSizeField });
			}
			else if(variableSizeField != null)
			{
				var stringValue = value as string;

				if(stringValue == null)
					throw new NotSupportedException("Unexpected variable size value type: " + value.GetType());

				_variableSizeWrites.Add(field, stringValue);
			}

			return this;
		}

		public override void Write(byte* ptr)
		{
			foreach (var fixedSizeWrite in _fixedSizeWrites)
			{
				var handle = GCHandle.Alloc(fixedSizeWrite.Value.Value, GCHandleType.Pinned);
				try
				{
					var fieldInfo = fixedSizeWrite.Value.FieldInfo;
					MemoryUtils.Copy(ptr + fieldInfo.Offset, (byte*) handle.AddrOfPinnedObject(), fieldInfo.Size);
				}
				finally
				{
					handle.Free();
				}
			}

			_fixedSizeWrites.Clear();


			ptr += _schema.FixedSize;


			// TODO arek - write in the following format [field_offsets][field_1_size|field_1_value][field_2_size|field_2_value]

			var intPtr = (int*) ptr;

			foreach (var write in _variableSizeWrites)
			{
				*intPtr = write.Value.Length;
				intPtr++;
			}

			ptr = (byte*) intPtr;

			foreach (var write in _variableSizeWrites)
			{
				fixed (char* stringPtr = write.Value)
				{
					MemoryUtils.Copy(ptr, (byte*) stringPtr, write.Value.Length);
				}
			}
		}

		public override int GetSize()
		{
			if (_schema.IsFixedSize)
				return _schema.FixedSize;

			return _schema.FixedSize +
			       sizeof (int)*_variableSizeWrites.Count + // variable fields lengths
			       _variableSizeWrites.Sum(x => x.Value.Length);
		}
	}
}