using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils;
using Jint;
using Jint.Native;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Json;
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

    internal static IndexField RetrieveCreateVectorField(string fieldName, object value)
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
            return new object[]{VectorValue.Null};

        var indexField = RetrieveCreateVectorField(fieldName, value);
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
            return new object[]{VectorValue.Null};

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

        object HandleEnumerable(IEnumerable enumerable, bool allowNestedArrays = true)
        {
            if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
                throw new InsufficientExecutionStackException($"Too many nested arrays in {nameof(CreateVector)}");
            
            List<object> vectorValues = new();
            if (enumerable is DynamicArray dynamicArray && CurrentIndexingScope.Current.Index.Type.IsMapReduce() && allowNestedArrays)
            {
                foreach (var item in dynamicArray.Inner)
                {
                    var reduce = HandleEnumerable(item as IEnumerable, false) as List<object>;
                    vectorValues.AddRange(reduce!);                        
                }

                return vectorValues;
            }
            
            var enumerator = enumerable.GetEnumerator();
            using var _ = enumerator as IDisposable;
            if (enumerator.MoveNext() == false)
            {
                vectorValues.Add(VectorValue.Null);
                return vectorValues;
            };

            // We've to find first non-null value do determine the underlying type of data.
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
                    case VectorEmbeddingType.Single when enumerator.Current is float[] itemAsFloats:
                    {
                        memScope = allocator.Allocate(itemAsFloats.Length * sizeof(float), out mem);
                        MemoryMarshal.Cast<float, byte>(itemAsFloats).CopyTo(mem.Span);
                        vectorValues.Add(GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, mem.Length));
                        break;
                    }
                    case VectorEmbeddingType.Single when enumerator.Current is DynamicArray itemAsFloatsDynamic:
                    {
                        List<float> itemAsFloats = new();
                        itemAsFloats.AddRange(itemAsFloatsDynamic.Inner.Select(x => (float)(LazyNumberValue)(object)x));
                        memScope = allocator.Allocate(itemAsFloats.Count * sizeof(float), out mem);
                        MemoryMarshal.Cast<float, byte>(CollectionsMarshal.AsSpan(itemAsFloats)).CopyTo(mem.Span);
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
                return new object[]{VectorValue.Null};

            var values = new object[dataLength];

            if (firstNonNull is BlittableJsonReaderVector)
            {
                for (var i = 0; i < dataLength; i++)
                {
                    values[i] = HandleBlittableJsonReaderVector(data[i] as BlittableJsonReaderVector);
                }

                return values;
            }
            
            if (firstNonNull is BlittableJsonReaderObject or DynamicBlittableJson)
            {
                for (int i = 0; i < dataLength; i++)
                {
                    var currentObject = data[i];
                    if (IsNullValue(currentObject))
                    {
                        values[i] = VectorValue.Null;
                        continue;
                    }
                    
                    var bjro = currentObject as BlittableJsonReaderObject ?? ((DynamicBlittableJson)value).BlittableJson;
                    if (bjro != null && bjro.TryGetMember(Sparrow.Global.Constants.Naming.VectorPropertyName, out var vector) 
                        && vector is BlittableJsonReaderVector bjrv)
                    {
                        values[i] = HandleBlittableJsonReaderVector(bjrv);
                        continue;
                    }
                    
                    PortableExceptions.Throw<InvalidDataException>($"Expected BlittableJsonReaderVector, but got {value.GetType().FullName}");
                }

                return values;
            }

            //Array of base64s
            if (IsBase64(firstNonNull))
            {
                for (var i = 0; i < dataLength; i++)
                    values[i] = Base64ToVector(data[i].ToString());

                return values;
            }

            //Array of arrays
            if (firstNonNull is BlittableJsonReaderArray)
            {
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
            if (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Int8)
            {
                if (bjrv.TryReadArray(out ReadOnlySpan<sbyte> asSbyte))
                    return HandleBjrvInternal(asSbyte);

                using (allocator.Allocate(bjrv.Length, out Span<sbyte> mem))
                {
                    var it = 0;
                    foreach (var itValue in bjrv.ReadAs<sbyte>())
                        mem[it++] = itValue;

                    return HandleBjrvInternal<sbyte>(mem);
                }
            }

            if (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Binary)
            {
                if (bjrv.TryReadArray(out ReadOnlySpan<byte> asBytes))
                    return HandleBjrvInternal(asBytes);

                using (allocator.Allocate(bjrv.Length, out Span<byte> mem))
                {
                    var it = 0;
                    foreach (var itValue in bjrv.ReadAs<byte>())
                        mem[it++] = itValue;

                    return HandleBjrvInternal<byte>(mem);
                }
            }

            if (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Single)
            {
                if (bjrv.TryReadArray(out ReadOnlySpan<float> asFloat))
                    return HandleBjrvInternal(asFloat);

                using (allocator.Allocate(bjrv.Length, out Span<float> mem))
                {
                    var it = 0;
                    foreach (var itValue in bjrv.ReadAs<float>())
                        mem[it++] = itValue;

                    return HandleBjrvInternal<float>(mem);
                }
            }
            
            throw new ArgumentException($"Unknown type. Vector embedding source: {vectorOptions.SourceEmbeddingType}");
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
            return IsNullValue(valueToProcess) 
                ? VectorValue.Null 
                : GenerateEmbeddings.FromText(allocator, indexField.Vector, GetStringFromObject(valueToProcess));
        }
    }

    private static string GetStringFromObject(object valueToProcess)
    {
        if (IsNullValue(valueToProcess))
            return null;
        
        return valueToProcess switch
        {
            LazyStringValue lsv => lsv,
            LazyCompressedStringValue lcsv => lcsv,
            string s => s,
            LazyJsString ljs => ljs.ToString(),
            JsString js => js.ToString(),
            _ => throw new NotSupportedException("Only strings are supported, but got: " + valueToProcess.GetType().FullName)
        };
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

    public static object LoadVectorJs(string fieldName, string embeddingGeneratorTaskName, string path, out IndexField vectorField, string documentId = null)
    {
        if (IsDictionaryTrainingPhase(CurrentIndexingScope.Current))
        {
            vectorField = null;
            return null;
        }
        
        var vectors = ProcessLoadVector(fieldName, embeddingGeneratorTaskName, path, out vectorField, documentId);

        //for js indexes we've no choice than create dynamic field, in such cases let's assume it is single. 
        if (vectorField == null)
        {
            vectorField = RetrieveCreateVectorField(fieldName, null);
            return new CoraxDynamicItem() { FieldName = fieldName, Field = vectorField, Value = vectors };
        }
        
        return (vectorField.Id == Corax.Constants.IndexWriter.DynamicField)
            ? new CoraxDynamicItem() { FieldName = fieldName, Field = vectorField, Value = vectors }
            : vectors;
    }
    
    public static object LoadVector(string fieldName, string embeddingGeneratorTaskIdentifier, string path, string documentId = null)
    {
        if (IsDictionaryTrainingPhase(CurrentIndexingScope.Current))
            return null;
        
        var vectors = ProcessLoadVector(fieldName, embeddingGeneratorTaskIdentifier, path, out var vectorField, documentId);

        if (vectorField == null)
            return vectors;
        
        return (vectorField.Id == Corax.Constants.IndexWriter.DynamicField)
            ? new CoraxDynamicItem() { FieldName = fieldName, Field = vectorField, Value = vectors }
            : vectors;
    }

    private static object ProcessLoadVector(string fieldName, string embeddingGeneratorTaskIdentifier, string path, out IndexField indexField, string documentId = null)
    {
        var currentIndexingScope = CurrentIndexingScope.Current;
        currentIndexingScope.Index.IndexFieldsPersistence.SetEmbeddingsGenerationTaskIdentifier(fieldName, embeddingGeneratorTaskIdentifier);
        var embeddingDocument = LoadVectorDocument(out var embeddingDocumentId, documentId) as DynamicBlittableJson;
        
        // no related document
        if (embeddingDocument == null
            // no embedding generator task in the document
            || BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, embeddingDocument.BlittableJson, embeddingGeneratorTaskIdentifier,
                out var documentEmbeddings) == false
            // no path in the embedding task dictionary
            || BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, (BlittableJsonReaderObject)documentEmbeddings, path,
                out var embeddingContainerObject) == false
            // stored value has no elements
            || IsNullValue(embeddingContainerObject))
        {
            indexField = null;
            return new object[]{VectorValue.Null};
        }

        if (currentIndexingScope.TryGetLoadVectorField(fieldName, out indexField) == false)
        {
            VectorEmbeddingType expectedVectorType = VectorEmbeddingType.Single;
            if (BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, (BlittableJsonReaderObject)documentEmbeddings,
                    Raven.Client.Constants.Documents.Metadata.Quantization, out var quantizationFromDocument))
            {
                var enumAsStr = GetStringFromObject(quantizationFromDocument);
                if (Enum.TryParse(enumAsStr.AsSpan(), out expectedVectorType) == false)
                    expectedVectorType = VectorEmbeddingType.Single;
            }
        
            indexField = currentIndexingScope.GetLoadVectorField(fieldName, new EmbeddingsGenerationTaskIdentifier(embeddingGeneratorTaskIdentifier), expectedVectorType);
        }
        
        
        if (embeddingContainerObject is BlittableJsonReaderArray bjra)
        {
            var attachmentNames = new string[bjra.Length];
            for (var i = 0; i < bjra.Length; i++)
                attachmentNames[i] = GetStringFromObject(bjra[i]);

            var attachments = currentIndexingScope.LoadAttachments(embeddingDocumentId, attachmentNames);
            return attachments is null 
                ? new object[]{ VectorValue.Null } 
                : VectorFromEmbedding(indexField, attachments.Select(x => x.GetContentAsStream()), isAutoIndex: false);
        }

        if (IsExplicitString(embeddingContainerObject))
        {
            var singleAttachmentName = GetStringFromObject(embeddingContainerObject);
            var attachment = currentIndexingScope.LoadAttachment(embeddingDocument, singleAttachmentName);
            return attachment is null 
                ? new object[]{ VectorValue.Null }
                : VectorFromEmbedding(indexField, attachment.GetContentAsStream(), isAutoIndex: false);
        }
        
        return new object[]{ VectorValue.Null };
    }
    
    private static dynamic LoadVectorDocument(out string embeddingDocument, string documentId = null)
    {
        var scope = CurrentIndexingScope.Current;
        
        var id = documentId ?? (string)scope.Source.GetId().ToString();
        embeddingDocument = EmbeddingsHelper.GetEmbeddingDocumentId(id);
        var collectionName = EmbeddingsHelper.GetEmbeddingDocumentCollectionName(scope.SourceCollection);
        return scope.LoadDocument(null, embeddingDocument, collectionName.ToLowerInvariant());
    }
}
