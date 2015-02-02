using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Voron
{
	public abstract class StructureField
	{
		public object Name;
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
		internal StructureField[] Fields = new StructureField[0];

		public StructureSchema()
		{
			var fieldType = typeof(TField);

			if (fieldType != typeof(Enum) && fieldType.IsEnum == false)
				throw new ArgumentException("IStructure schema can only have fields of enum type.");


			IsFixedSize = true;
		}

		public bool IsFixedSize { get; private set; }

		public int FixedSize { get { return _fixedFieldOffset; } }

		public int VariableFieldsCount { get { return _variableFieldIndex; } }

		private void AddField(int index, StructureField field)
		{
			if (index >= Fields.Length)
			{
				var biggerArray = new StructureField[index + 1];
				Array.Copy(Fields, biggerArray, Fields.Length);
				Fields = biggerArray;
			}

			Fields[index] = field;
		}

		public StructureField this[int index]
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)] 
			get
			{
				return index >= Fields.Length ? null : Fields[index];
			}
		}

		public StructureSchema<TField> Add<T>(TField field)
		{
			var index = (int) (object) field;

			if (Fields.Length - 1 >= index && Fields[index] != null)
				throw new ArgumentException(string.Format("Field '{0}' is already defined", field));

			var type = typeof (T);

			if (type == typeof(string) || type == typeof(byte[]))
			{
				IsFixedSize = false;

				AddField(index, new VariableSizeField
				{
					Name = field,
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

				AddField(index, new FixedSizeField
				{
					Name = field,
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