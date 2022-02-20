using System;
using System.Diagnostics;
using System.Reflection;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;

namespace Voron.Data.Tables
{
    public unsafe partial class TableSchema
    {
        [Flags]
        public enum TreeIndexType
        {
            Default = 0x01,
            DynamicKeyValues = 0x2
        }

        public abstract class AbstractBTreeIndexDef
        {
            public abstract TreeIndexType Type { get; }

            public bool IsGlobal;

            public Slice Name;

            public abstract ByteStringContext.Scope GetSlice(ByteStringContext context, ref TableValueReader value,
                out Slice slice);

            public abstract ByteStringContext.Scope GetSlice(ByteStringContext context, TableValueBuilder value,
                out Slice slice);

            public abstract byte[] Serialize();

            public abstract void Validate(AbstractBTreeIndexDef actual);

            public virtual void Validate()
            {
                if (Name.HasValue == false || SliceComparer.Equals(Slices.Empty, Name))
                    throw new ArgumentException("Index name must be non-empty", nameof(Name));
            }

        }

        public class StaticBTreeIndexDef : AbstractBTreeIndexDef
        {
            public override TreeIndexType Type => TreeIndexType.Default;

            /// <summary>
            /// Here we take advantage on the fact that the values are laid out in memory sequentially
            /// we can point to a certain item index, and use one or more fields in the key directly, 
            /// without any copying
            /// </summary>
            public int StartIndex = -1;

            public int Count = -1;

            public override ByteStringContext.Scope GetSlice(ByteStringContext context, ref TableValueReader value,
                out Slice slice)
            {
                var ptr = value.Read(StartIndex, out int totalSize);
#if DEBUG
                if (totalSize < 0)
                    throw new ArgumentOutOfRangeException(nameof(totalSize), "Size cannot be negative");
#endif
                for (var i = 1; i < Count; i++)
                {
                    int size;
                    value.Read(i + StartIndex, out size);
#if DEBUG
                    if (size < 0)
                        throw new ArgumentOutOfRangeException(nameof(size), "Size cannot be negative");
#endif
                    totalSize += size;
                }
#if DEBUG
                if (totalSize < 0 || totalSize > value.Size)
                    throw new ArgumentOutOfRangeException(nameof(value), "Reading a slice that is longer than the value");
                if (totalSize > ushort.MaxValue)
                    throw new ArgumentOutOfRangeException(nameof(totalSize),
                        "Reading a slice that too big to be a slice");
#endif
                return Slice.External(context, ptr, (ushort)totalSize, out slice);
            }

            public override ByteStringContext.Scope GetSlice(ByteStringContext context, TableValueBuilder value,
                out Slice slice)
            {
                if (Count == 1)
                    return value.SliceFromLocation(context, StartIndex, out slice);

                int totalSize = value.SizeOf(StartIndex);
                for (int i = 1; i < Count; i++)
                {
                    totalSize += value.SizeOf(i + StartIndex);
                }
#if DEBUG
                if (totalSize < 0)
                    throw new ArgumentOutOfRangeException(nameof(totalSize), "Size cannot be negative");
#endif
                var scope = context.Allocate(totalSize, out ByteString ret);
                try
                {
                    var ptr = ret.Ptr;
                    Slice val;
                    using (value.SliceFromLocation(context, StartIndex, out val))
                    {
                        val.CopyTo(ptr);
                        ptr += val.Size;
                    }
                    for (var i = 1; i < Count; i++)
                    {
                        using (value.SliceFromLocation(context, i + StartIndex, out val))
                        {
                            val.CopyTo(ptr);
                            ptr += val.Size;
                        }
                    }
                    slice = new Slice(ret);
                    return scope;
                }
                catch (Exception)
                {
                    scope.Dispose();
                    throw;
                }
            }

            public override byte[] Serialize()
            {
                // We serialize the Type enum as ulong to be "future-proof"
                var castedType = (long)Type;

                var serializer = new TableValueBuilder
                {
                    castedType,
                    StartIndex,
                    Count,
                    IsGlobal,
                    Name
                };

                byte[] serialized = new byte[serializer.Size];

                fixed (byte* destination = serialized)
                {
                    serializer.CopyTo(destination);
                }

                return serialized;
            }

            public override void Validate(AbstractBTreeIndexDef actual)
            {
                if (actual == null)
                    throw new ArgumentNullException(nameof(actual), "Expected an index but received null");

                if (!SliceComparer.Equals(Name, actual.Name))
                    throw new ArgumentException(
                        $"Expected index to have Name='{Name}', got Name='{actual.Name}' instead",
                        nameof(actual));

                if (IsGlobal != actual.IsGlobal)
                    throw new ArgumentException(
                        $"Expected index {Name} to have IsGlobal='{IsGlobal}', got IsGlobal='{actual.IsGlobal}' instead",
                        nameof(actual));

                if (Type != actual.Type || actual is not StaticBTreeIndexDef staticBTreeIndexDef)
                    throw new ArgumentException(
                        $"Expected index {Name} to have Type='{Type}', got Type='{actual.Type}' instead",
                        nameof(actual));

                if (StartIndex != staticBTreeIndexDef.StartIndex)
                    throw new ArgumentException(
                        $"Expected index {Name} to have StartIndex='{StartIndex}', got StartIndex='{staticBTreeIndexDef.StartIndex}' instead",
                        nameof(actual));

                if (Count != staticBTreeIndexDef.Count)
                    throw new ArgumentException(
                        $"Expected index {Name} to have Count='{Count}', got Count='{staticBTreeIndexDef.Count}' instead",
                        nameof(actual));
            }

            public override void Validate()
            {
                base.Validate();

                if (StartIndex < 0)
                    throw new ArgumentOutOfRangeException(nameof(StartIndex), "StartIndex cannot be negative");
            }

            public static StaticBTreeIndexDef ReadFrom(ByteStringContext context, byte* location, int size)
            {
                var input = new TableValueReader(location, size);
                var indexDef = new StaticBTreeIndexDef();

                byte* currentPtr = input.Read(1, out int currentSize);
                indexDef.StartIndex = *(int*)currentPtr;

                currentPtr = input.Read(2, out currentSize);
                indexDef.Count = *(int*)currentPtr;

                currentPtr = input.Read(3, out currentSize);
                indexDef.IsGlobal = Convert.ToBoolean(*currentPtr);

                currentPtr = input.Read(4, out currentSize);
                Slice.From(context, currentPtr, currentSize, ByteStringType.Immutable, out indexDef.Name);

                return indexDef;
            }
        }

        public class FixedSizeTreeIndexDef
        {
            public int StartIndex = -1;
            public bool IsGlobal;
            public Slice Name;

            public long GetValue(ref TableValueReader value)
            {
                var ptr = value.Read(StartIndex, out int totalSize);
                Debug.Assert(totalSize == sizeof(long), $"{totalSize} == sizeof(long) - {Name}");
                return Bits.SwapBytes(*(long*)ptr);
            }

            public long GetValue(ByteStringContext context, TableValueBuilder value)
            {
                using (value.SliceFromLocation(context, StartIndex, out Slice slice))
                {
                    return Bits.SwapBytes(*(long*)slice.Content.Ptr);
                }
            }

            public byte[] Serialize()
            {
                var serializer = new TableValueBuilder
                {
                    StartIndex,
                    IsGlobal,
                    Name
                };

                byte[] serialized = new byte[serializer.Size];

                fixed (byte* destination = serialized)
                {
                    serializer.CopyTo(destination);
                }

                return serialized;
            }

            public static FixedSizeTreeIndexDef ReadFrom(ByteStringContext context, byte* location, int size)
            {
                var input = new TableValueReader(location, size);
                var output = new FixedSizeTreeIndexDef();

                int currentSize;
                byte* currentPtr = input.Read(0, out currentSize);
                output.StartIndex = *(int*)currentPtr;

                currentPtr = input.Read(1, out currentSize);
                output.IsGlobal = Convert.ToBoolean(*currentPtr);

                currentPtr = input.Read(2, out currentSize);
                Slice.From(context, currentPtr, currentSize, ByteStringType.Immutable, out output.Name);

                return output;
            }

            public void Validate(FixedSizeTreeIndexDef actual)
            {
                if (actual == null)
                    throw new ArgumentNullException(nameof(actual), "Expected an index but received null");

                if (!SliceComparer.Equals(Name, actual.Name))
                    throw new ArgumentException(
                        $"Expected index to have Name='{Name}', got Name='{actual.Name}' instead",
                        nameof(actual));

                if (StartIndex != actual.StartIndex)
                    throw new ArgumentException(
                        $"Expected index {Name} to have StartIndex='{StartIndex}', got StartIndex='{actual.StartIndex}' instead",
                        nameof(actual));

                if (IsGlobal != actual.IsGlobal)
                    throw new ArgumentException(
                        $"Expected index {Name} to have IsGlobal='{IsGlobal}', got IsGlobal='{actual.IsGlobal}' instead",
                        nameof(actual));
            }
        }

        public class DynamicBTreeIndexDef : AbstractBTreeIndexDef
        {
            public override TreeIndexType Type => TreeIndexType.DynamicKeyValues;

            public delegate ByteStringContext.Scope IndexValueAction(ByteStringContext context, ref TableValueReader value, out Slice slice);

            public IndexValueAction IndexValueGenerator;

            private static readonly Assembly[] Assemblies;

            static DynamicBTreeIndexDef()
            {
                Assemblies = AppDomain.CurrentDomain.GetAssemblies();
            }

            public override ByteStringContext.Scope GetSlice(ByteStringContext context, ref TableValueReader value,
                out Slice slice)
            {
                return IndexValueGenerator(context, ref value, out slice);
            }

            public override ByteStringContext.Scope GetSlice(ByteStringContext context, TableValueBuilder value,
                out Slice slice)
            {
                using (context.Allocate(value.Size, out var buffer))
                {
                    value.CopyTo(buffer.Ptr);
                    var reader = value.CreateReader(buffer.Ptr);
                    return IndexValueGenerator(context, ref reader, out slice);
                }
            }

            public override byte[] Serialize()
            {
                // We serialize the Type enum as ulong to be "future-proof"
                var castedType = (long)Type;

                var serializer = new TableValueBuilder
                {
                    castedType,
                    IsGlobal,
                    Name
                };

                var methodNameBytes = Encodings.Utf8.GetBytes(IndexValueGenerator.Method.Name);
                fixed (byte* ptr = methodNameBytes)
                {
                    serializer.Add(ptr, methodNameBytes.Length);
                }

                Debug.Assert(IndexValueGenerator.Method.DeclaringType?.FullName != null,
                    $"Invalid {nameof(IndexValueGenerator)} '{IndexValueGenerator.Method.Name}'");

                var declaringTypeBytes = Encodings.Utf8.GetBytes(IndexValueGenerator.Method.DeclaringType.FullName);
                fixed (byte* ptr = declaringTypeBytes)
                {
                    serializer.Add(ptr, declaringTypeBytes.Length);
                }

                byte[] serialized = new byte[serializer.Size];

                fixed (byte* destination = serialized)
                {
                    serializer.CopyTo(destination);
                }

                return serialized;
            }

            public static DynamicBTreeIndexDef ReadFrom(ByteStringContext context, byte* location, int size)
            {
                var input = new TableValueReader(location, size);
                var indexDef = new DynamicBTreeIndexDef();

                byte* currentPtr = input.Read(1, out _);
                indexDef.IsGlobal = Convert.ToBoolean(*currentPtr);

                currentPtr = input.Read(2, out var currentSize);
                Slice.From(context, currentPtr, currentSize, ByteStringType.Immutable, out indexDef.Name);

                // read IndexValueGenerator method name
                currentPtr = input.Read(3, out currentSize);
                var methodName = Encodings.Utf8.GetString(currentPtr, currentSize);

                // read IndexValueGenerator declaring type
                currentPtr = input.Read(4, out currentSize);
                var declaringType = Encodings.Utf8.GetString(currentPtr, currentSize);

                //var type = System.Type.GetType(declaringType);
                var type = System.Type.GetType(declaringType);

                if (type == null)
                {
                    foreach (var assembly in Assemblies)
                    {
                        type = assembly.GetType(declaringType);
                        if (type != null)
                            break;
                    }
                }
                Debug.Assert(type != null, $"Invalid data, failed to get IndexValueGenerator.Method.DeclaringType from serialized value : {declaringType}");


                var method = type.GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static);
                Debug.Assert(method != null, $"Invalid data, failed to get method-info from type : {type}, method name : {methodName}");
                Debug.Assert(method.IsStatic, $"Invalid data, IndexValueGenerator must be a static method. method name : {methodName}");

                var @delegate = Delegate.CreateDelegate(typeof(IndexValueAction), method);
                indexDef.IndexValueGenerator = (IndexValueAction)@delegate;

                return indexDef;
            }

            public override void Validate(AbstractBTreeIndexDef actual)
            {
                if (actual == null)
                    throw new ArgumentNullException(nameof(actual), "Expected an index but received null");

                if (!SliceComparer.Equals(Name, actual.Name))
                    throw new ArgumentException(
                        $"Expected index to have Name='{Name}', got Name='{actual.Name}' instead",
                        nameof(actual));

                if (IsGlobal != actual.IsGlobal)
                    throw new ArgumentException(
                        $"Expected index {Name} to have IsGlobal='{IsGlobal}', got IsGlobal='{actual.IsGlobal}' instead",
                        nameof(actual));

                if (Type != actual.Type || actual is not DynamicBTreeIndexDef dynamicIndexDef)
                    throw new ArgumentException(
                        $"Expected index {Name} to have Type='{Type}', got Type='{actual.Type}' instead",
                        nameof(actual));

                if (IndexValueGenerator.Method.Name != dynamicIndexDef.IndexValueGenerator.Method.Name)
                    throw new ArgumentException(
                        $"Expected index {Name} to have IndexValueGenerator.Method.Name='{IndexValueGenerator.Method.Name}', got IndexValueGenerator.Method.Name='{dynamicIndexDef.IndexValueGenerator.Method.Name}' instead",
                        nameof(actual));

                if (IndexValueGenerator.Method.DeclaringType != dynamicIndexDef.IndexValueGenerator.Method.DeclaringType)
                    throw new ArgumentException(
                        $"Expected index {Name} to have IndexValueGenerator.Method.DeclaringType='{IndexValueGenerator.Method.DeclaringType}', got IndexValueGenerator.Method.DeclaringType='{dynamicIndexDef.IndexValueGenerator.Method.DeclaringType}' instead",
                        nameof(actual));
            }

            public override void Validate()
            {
                base.Validate();

                if (IndexValueGenerator == null)
                    throw new ArgumentOutOfRangeException(nameof(IndexValueGenerator), "IndexValueGenerator delegate cannot be null");

                if (IndexValueGenerator.Method.DeclaringType == null)
                    throw new ArgumentOutOfRangeException(nameof(IndexValueGenerator), "IndexValueGenerator.Method.DeclaringType cannot be null");

                if (IndexValueGenerator.Method.IsStatic == false)
                    throw new ArgumentOutOfRangeException(nameof(IndexValueGenerator), "IndexValueGenerator must be a static method");
            }
        }
    }
}
