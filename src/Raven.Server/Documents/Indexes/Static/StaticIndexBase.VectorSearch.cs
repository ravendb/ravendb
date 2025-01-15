using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils;
using Jint;
using Jint.Native;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static;

public partial class AbstractStaticIndexBase
{
    /// <summary>
    /// Dictionary training process occurs in IsOnBeforeExecuteIndexing. Since we're not training dictionaries with vectors, and considering computation
    /// power required (e.g., generating embeddings from text), it is better to skip that part as there is no benefit in performing it.
    /// </summary>
    /// <param name="currentIndexingScope">Current indexing scope.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsDictionaryTrainingPhase(CurrentIndexingScope currentIndexingScope)
    {
        return currentIndexingScope != null && currentIndexingScope.Index.IsOnBeforeExecuteIndexing;
    }

    internal static IndexField RetrieveVectorField(string fieldName, object value)
    {
        var currentIndexingScope = CurrentIndexingScope.Current;
        var fieldExists = currentIndexingScope.Index.Definition.IndexFields.TryGetValue(fieldName, out var indexField);

        if (fieldExists && IsNullValue(value))
            return indexField;

        if (fieldExists == false || indexField?.Vector is null)
        {
            // We're supporting two defaults:
            // when Options are not set, we'll decide what is configuration in following manner:
            // - value is textual or array of textual we're treating them as text input
            // - otherwise, we will write as array of numerical values
            var isText = IsExplicitString(value);
            if (isText == false && value is IEnumerable enumerable)
            {
                foreach (var item in enumerable)
                {
                    if (IsNullValue(item))
                        continue;

                    isText = IsExplicitString(item);
                    break;
                }
            }

            indexField = currentIndexingScope.GetOrCreateVectorField(fieldName, isText);

            if (indexField.Id == Corax.Constants.IndexWriter.DynamicField)
            {
                currentIndexingScope.DynamicFields ??= new Dictionary<string, IndexField>();
                if (currentIndexingScope.DynamicFields.TryAdd(fieldName, indexField))
                    currentIndexingScope.IncrementDynamicFields();
            }
        }

        PortableExceptions.ThrowIf<InvalidDataException>(indexField?.Vector is null,
            $"Field '{fieldName}' does not exist in this indexing scope. Cannot index as vector.");

        indexField!.Vector!.ValidateDebug();

        return indexField;
    }

    public object CreateVector(string fieldName, object value)
    {
        var currentIndexingScope = CurrentIndexingScope.Current;
        if (IsDictionaryTrainingPhase(currentIndexingScope) || IsNullValue(value))
            return VectorValue.Null;

        var indexField = RetrieveVectorField(fieldName, value);
        var vector = indexField!.Vector!.SourceEmbeddingType switch
        {
            VectorEmbeddingType.Text => VectorFromText(indexField, value),
            _ => VectorFromEmbedding(indexField, value)
        };

        return indexField.Id != Corax.Constants.IndexWriter.DynamicField ? vector : new CoraxDynamicItem() { Field = indexField, Value = vector };
    }

    /// <summary>
    /// Create vector field object. This method is used by AutoIndexes and JavaScript indexes.
    /// </summary>
    /// <param name="indexField">IndexField from IndexDefinition</param>
    /// <param name="value">Data source to create vector field.</param>
    /// <returns></returns>
    internal static object CreateVector(IndexField indexField, object value, bool isAutoIndex)
    {
        if (IsDictionaryTrainingPhase(CurrentIndexingScope.Current) || IsNullValue(value))
            return VectorValue.Null;

        return indexField!.Vector!.SourceEmbeddingType switch
        {
            VectorEmbeddingType.Text => VectorFromText(indexField, value),
            _ => VectorFromEmbedding(indexField, value, isAutoIndex)
        };
    }

    private static object VectorFromEmbedding(IndexField currentIndexingField, object value, bool isAutoIndex = false)
    {
        var vectorOptions = currentIndexingField.Vector;
        var allocator = CurrentIndexingScope.Current.IndexContext.Allocator;

        if (IsExplicitString(value))
            return Base64ToVector(value);

        switch (value)
        {
            case BlittableJsonReaderArray or DynamicArray { Inner: BlittableJsonReaderArray }:
            {
                var bjra = value as BlittableJsonReaderArray ?? (BlittableJsonReaderArray)((DynamicArray)value).Inner;
                return HandleBlittableJsonReaderArray(bjra);
            }
            case BlittableJsonReaderObject or DynamicBlittableJson:
            {
                var bjro = value as BlittableJsonReaderObject ?? ((DynamicBlittableJson)value).BlittableJson;
                if (bjro.TryGetMember(Sparrow.Global.Constants.Naming.VectorPropertyName, out var vector) && vector is BlittableJsonReaderVector bjrv)
                {
                    return HandleBlittableJsonReaderVector(bjrv);
                }

                PortableExceptions.Throw<ArgumentException>($"Expected BlittableJsonReaderVector, but got {value.GetType().FullName}");
                break;
            }
            case Stream stream:
                return HandleStream(stream);
            case JsArray js:
                return HandleJsArray(js);
            case IEnumerable ie:
                return HandleEnumerable(ie);
        }

        throw new ArgumentException($"Unknown type. Value type: {value.GetType().FullName}");
        
        object Base64ToVector(object base64)
        {
            var str = base64.ToString();
            return GenerateEmbeddings.FromBase64Array(vectorOptions, allocator, str, isAutoIndex);
        }

        object HandleEnumerable(IEnumerable enumerable)
        {
            var enumerator = enumerable.GetEnumerator();
            using var _ = enumerator as IDisposable;
            if (enumerator.MoveNext() == false)
                return VectorValue.Null;

            // We've to find first non-null value do determine the underlying type of data.
            List<object> vectorValues = new();
            while (IsNullValue(enumerator.Current))
            {
                vectorValues.Add(VectorValue.Null);
                if (enumerator.MoveNext() == false)
                    return vectorValues;
            }

            var isBase64 = IsBase64(enumerator.Current);
            var isStream = enumerator.Current is Stream;
            do
            {
                if (isBase64)
                {
                    vectorValues.Add(Base64ToVector(enumerator.Current));
                    continue;
                }

                if (isStream)
                {
                    vectorValues.Add(HandleStream((Stream)enumerator.Current));
                    continue;
                }

                IDisposable memScope;
                Memory<byte> mem;
                switch (vectorOptions.SourceEmbeddingType)
                {
                    case VectorEmbeddingType.Single:
                    {
                        var itemAsFloats = (float[])enumerator.Current!;
                        memScope = allocator.Allocate(itemAsFloats.Length * sizeof(float), out mem);
                        MemoryMarshal.Cast<float, byte>(itemAsFloats).CopyTo(mem.Span);
                        vectorValues.Add(GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, mem.Length));
                        break;
                    }
                    case VectorEmbeddingType.Int8:
                    {
                        var itemAsSbytes = (sbyte[])enumerator.Current!;
                        memScope = allocator.Allocate(itemAsSbytes.Length, out mem);
                        MemoryMarshal.Cast<sbyte, byte>(itemAsSbytes).CopyTo(mem.Span);
                        vectorValues.Add(GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, mem.Length));
                        break;
                    }
                    default:
                    {
                        var item = (byte[])enumerator.Current!;
                        memScope = allocator.Allocate(item.Length, out mem);
                        item.CopyTo(mem.Span);
                        vectorValues.Add(GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, mem.Length));
                        break;
                    }
                }
            } while (enumerator.MoveNext());

            return vectorValues;
        }

        object HandleBlittableJsonReaderArray(BlittableJsonReaderArray data)
        {
            var dataLength = data.Length;

            if (TryGetFirstNonNullElement(data, out var firstNonNull) == false)
                return VectorValue.Null;

            //Array of base64s
            if (IsBase64(firstNonNull))
            {
                var values = new object[dataLength];
                for (var i = 0; i < dataLength; i++)
                    values[i] = Base64ToVector(data[i].ToString());

                return values;
            }

            //Array of arrays
            if (firstNonNull is BlittableJsonReaderArray)
            {
                var values = new object[dataLength];
                for (var i = 0; i < dataLength; i++)
                {
                    values[i] = IsNullValue(data[i]) 
                        ? VectorValue.Null 
                        : HandleBlittableJsonReaderArray((BlittableJsonReaderArray)data[i]);
                }

                return values;
            }

            var bufferSize = dataLength * (vectorOptions.SourceEmbeddingType) switch
            {
                VectorEmbeddingType.Single => sizeof(float),
                _ => sizeof(byte)
            };

            var memScope = allocator.Allocate(bufferSize, out Memory<byte> mem);
            ref var floatRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(mem.Span));
            ref var sbyteRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, sbyte>(mem.Span));
            ref var byteRef = ref MemoryMarshal.GetReference(mem.Span);

            for (int i = 0; i < dataLength; ++i)
            {
                switch (vectorOptions.SourceEmbeddingType)
                {
                    case VectorEmbeddingType.Single:
                        Unsafe.Add(ref floatRef, i) = data.GetByIndex<float>(i);
                        break;
                    case VectorEmbeddingType.Int8:
                        Unsafe.Add(ref sbyteRef, i) = data.GetByIndex<sbyte>(i);
                        break;
                    default:
                        Unsafe.AddByteOffset(ref byteRef, i) = data.GetByIndex<byte>(i);
                        break;
                }
            }

            return GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, bufferSize);
        }

        object HandleBlittableJsonReaderVector(BlittableJsonReaderVector bjrv)
        {
            var type = bjrv[0].GetType();

            if (type == typeof(float))
                return HandleBjrvInternal(bjrv.ReadArray<float>());
            if (type == typeof(double))
                return HandleBjrvInternal(bjrv.ReadArray<double>());
            if (type == typeof(byte))
                return HandleBjrvInternal(bjrv.ReadArray<byte>());
            if (type == typeof(sbyte))
                return HandleBjrvInternal(bjrv.ReadArray<sbyte>());

            throw new NotSupportedException($"Embeddings of type {type.FullName} are not supported.");
        }

        object HandleBjrvInternal<T>(ReadOnlySpan<T> embedding) where T : unmanaged
        {
            var bufferSize = embedding.Length * (vectorOptions.SourceEmbeddingType) switch
            {
                VectorEmbeddingType.Single => sizeof(float),
                _ => sizeof(byte)
            };

            var memScope = allocator.Allocate(bufferSize, out Memory<byte> mem);

            MemoryMarshal.Cast<T, byte>(embedding).CopyTo(mem.Span);

            return GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, bufferSize);
        }

        object HandleJsArray(JsArray jsArray)
        {
            var firstItem = jsArray[0];
            if (firstItem.IsString())
            {
                var values = new object[jsArray.Length];
                for (var i = 0; i < jsArray.Length; i++)
                    values[i] = GenerateEmbeddings.FromBase64Array(vectorOptions, allocator, jsArray[i].AsString());

                return values;
            }

            if (firstItem is JsArray)
            {
                var values = new object[jsArray.Length];
                for (var i = 0; i < jsArray.Length; i++)
                    values[i] = HandleJsArray(jsArray[i] as JsArray);

                return values;
            }

            var len = (int)jsArray.Length;
            var bufferSize = len * (vectorOptions.SourceEmbeddingType) switch
            {
                VectorEmbeddingType.Single => sizeof(float),
                _ => sizeof(byte)
            };

            var memScope = allocator.Allocate(bufferSize, out Memory<byte> mem);
            ref var floatRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(mem.Span));
            ref var sbyteRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, sbyte>(mem.Span));
            ref var byteRef = ref MemoryMarshal.GetReference(mem.Span);

            for (int i = 0; i < len; ++i)
            {
                var num = jsArray[i].AsNumber();
                switch (vectorOptions.SourceEmbeddingType)
                {
                    case VectorEmbeddingType.Single:
                        Unsafe.Add(ref floatRef, i) = (float)num;
                        break;
                    case VectorEmbeddingType.Int8:
                        Unsafe.Add(ref sbyteRef, i) = Convert.ToSByte(num);
                        break;
                    default:
                        Unsafe.AddByteOffset(ref byteRef, i) = Convert.ToByte(num);
                        break;
                }
            }

            return GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, bufferSize);
        }

        object HandleStream(Stream stream)
        {
            var len = (int)stream.Length;
            var memScope = allocator.Allocate((int)stream.Length, out Memory<byte> mem);
            stream.ReadExactly(mem.Span);
            return GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, len);
        }

        bool IsBase64(object val) => CanTransformIntoString(val);

        bool TryGetFirstNonNullElement(BlittableJsonReaderArray data, out object firstNonNull)
        {
            firstNonNull = data[0];

            var i = 0;
            while (IsNullValue(firstNonNull) && i < data.Length)
                firstNonNull = data[i++];

            return IsNullValue(firstNonNull) == false;
        }
    }

    private static object VectorFromText(IndexField indexField, object value)
    {
        var allocator = CurrentIndexingScope.Current.IndexContext.Allocator;

        if (CanTransformIntoString(value))
            return CreateVectorValue(value);

        PortableExceptions.ThrowIfNot<ArgumentException>(value is IEnumerable,
            $"Tried to convert text into embeddings but got type {value?.GetType().FullName} which is not supported.");

        var vectorList = new List<VectorValue>();
        foreach (var item in ((IEnumerable)value)!)
        {
            vectorList.Add(CreateVectorValue(item));
        }

        return vectorList;

        VectorValue CreateVectorValue(object valueToProcess)
        {
            if (IsNullValue(valueToProcess))
                return VectorValue.Null;

            var str = valueToProcess switch
            {
                LazyStringValue lsv => lsv,
                LazyCompressedStringValue lcsv => lcsv,
                string s => s,
                LazyJsString ljs => ljs.ToString(),
                _ => throw new NotSupportedException("Only strings are supported, but got: " + valueToProcess.GetType().FullName)
            };

            return GenerateEmbeddings.FromText(allocator, indexField.Vector, str);
        }
    }

    /// <summary>
    /// Determines if the given value is explicitly a string-like type.
    /// </summary>
    private static bool IsExplicitString(object value) => value
        is LazyStringValue
        or LazyCompressedStringValue
        or string
        or LazyJsString
        or JsString;

    /// <summary>
    /// Determines if the given item can be converted into a string-like type.
    /// </summary>
    private static bool CanTransformIntoString(object item) => IsExplicitString(item)
                                                               || IsNullValue(item);

    /// <summary>
    /// Determines if the given value is explicitly a null type
    /// </summary>
    private static bool IsNullValue(object value)
    {
        return value is null or DynamicNullObject or DynamicJsNull or JsNull;
    }
}
