using System;
using System.Collections.Generic;
using Voron.Impl;

namespace Voron
{
	public abstract class StructureField
	{
		public Type Type;
	}

	public class FixedSizeField : StructureField
	{
		public int Offset;
		public int Size;
	}

	public class VariableSizeField : StructureField
	{
		public int Index;
	}

	public class StructureSchema<TField>
	{
		private int _fixedFieldOffset = 0;
		private int _variableFieldIndex = 0;
		internal readonly Dictionary<TField, FixedSizeField> _fixedSizeFields = new Dictionary<TField, FixedSizeField>();
		internal readonly Dictionary<TField, VariableSizeField> _variableSizeFields = new Dictionary<TField, VariableSizeField>();

		public StructureSchema()
		{
			IsFixedSize = true;
		}

		public bool IsFixedSize { get; private set; }

		public int FixedSize { get { return _fixedFieldOffset; } }

		public StructureSchema<TField> Add<T>(TField field)
		{
			var type = typeof(T);

			if (type == typeof(bool))
				throw new ArgumentException("bool is the non-blittable type");

			if (type == typeof(string))
			{
				IsFixedSize = false;

				_variableSizeFields.Add(field, new VariableSizeField
				{
					Type = type,
					Index = _variableFieldIndex
				});

				_variableFieldIndex++;
			}
			else if (type.IsPrimitive)
			{
				if (IsFixedSize == false)
					throw new ArgumentException("Cannot define fixed size fields after fields of variable size");

				var size = SizeOf.Primitive(type);

				_fixedSizeFields.Add(field, new FixedSizeField
				{
					Type = type,
					Offset = _fixedFieldOffset,
					Size = size
				});

				_fixedFieldOffset += size;
			}
			else
				throw new NotSupportedException("Not supported structure field type: " + type);

			return this;
		}
	}
}