using System;
using System.Collections.Generic;

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
		private static readonly Dictionary<Type, int> SizeOfPrimitives = new Dictionary<Type, int>
		{
			{typeof (sbyte), sizeof (sbyte)},
			{typeof (byte), sizeof (byte)},
			{typeof (short), sizeof (short)},
			{typeof (ushort), sizeof (ushort)},
			{typeof (int), sizeof (int)},
			{typeof (uint), sizeof (uint)},
			{typeof (long), sizeof (long)},
			{typeof (ulong), sizeof (ulong)},
			{typeof (char), sizeof (char)},
			{typeof (float), sizeof (float)},
			{typeof (double), sizeof (double)},
			{typeof (decimal), sizeof (decimal)},
			{typeof (bool), sizeof (byte)}, // bool is non-blittable so we store it as byte
		};

		private int _fixedFieldOffset = 0;
		private int _variableFieldIndex = 0;
		internal readonly Dictionary<TField, FixedSizeField> _fixedSizeFields = new Dictionary<TField, FixedSizeField>();
		internal readonly Dictionary<TField, VariableSizeField> _variableSizeFields = new Dictionary<TField, VariableSizeField>();

		public StructureSchema()
		{
			var fieldType = typeof(TField);

			if(fieldType != typeof(string) && fieldType != typeof(Enum) && fieldType.IsEnum == false && fieldType.IsPrimitive == false)
				throw new ArgumentException("IStructure schema can have fields of the following types: string, enum, primitives.");

			IsFixedSize = true;
		}

		public bool IsFixedSize { get; private set; }

		public int FixedSize { get { return _fixedFieldOffset; } }

		public StructureSchema<TField> Add<T>(TField field)
		{
			var type = typeof(T);

			if(_fixedSizeFields.ContainsKey(field) || _variableSizeFields.ContainsKey(field))
				throw new ArgumentException(string.Format("Field '{0}' is already defined", field));

			if (type == typeof(string) || type == typeof(byte[]))
			{
				IsFixedSize = false;

				_variableSizeFields.Add(field, new VariableSizeField
				{
					Type = type,
					Index = _variableFieldIndex
				});

				_variableFieldIndex++;
			}
			else if (type.IsPrimitive || type == typeof(decimal))
			{
				if (IsFixedSize == false)
					throw new ArgumentException("Cannot define a fixed size field after variable size fields");

				var size = SizeOfPrimitives[type];

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