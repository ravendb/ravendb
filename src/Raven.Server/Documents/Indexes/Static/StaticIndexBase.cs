using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Logging;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Spatial4n.Shapes;

namespace Raven.Server.Documents.Indexes.Static
{
    public delegate IEnumerable IndexingFunc(IEnumerable<dynamic> items);

    public abstract class StaticCountersIndexBase : StaticCountersAndTimeSeriesIndexBase
    {
    }

    public abstract class StaticTimeSeriesIndexBase : StaticCountersAndTimeSeriesIndexBase
    {
    }

    public abstract class StaticCountersAndTimeSeriesIndexBase : AbstractStaticIndexBase
    {
        public void AddMap(string collection, string name, IndexingFunc map)
        {
            AddMapInternal(collection, name, map);
        }
    }

    public abstract class StaticIndexBase : AbstractStaticIndexBase
    {
        public void AddMap(string collection, IndexingFunc map)
        {
            AddMapInternal(collection, collection, map);
        }

        public dynamic LoadAttachments(dynamic doc)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized.");

            if (doc is DynamicNullObject)
                return DynamicNullObject.Null;

            var document = doc as DynamicBlittableJson;
            if (document == null)
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a parameter, but was called with a parameter of type {doc.GetType().FullName}: {doc}");

            return CurrentIndexingScope.Current.LoadAttachments(document);
        }

        public dynamic LoadAttachment(dynamic doc, object attachmentName)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized. Attachment Name: {attachmentName}");

            if (doc is DynamicNullObject)
                return DynamicNullObject.Null;

            var document = doc as DynamicBlittableJson;
            if (document == null)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a non-null entity as a first parameter, but was called with a parameter of type {doc.GetType().FullName}: {doc}");

            if (attachmentName is LazyStringValue attachmentNameLazy)
                return CurrentIndexingScope.Current.LoadAttachment(document, attachmentNameLazy);

            if (attachmentName is string attachmentNameString)
                return CurrentIndexingScope.Current.LoadAttachment(document, attachmentNameString);

            if (attachmentName is DynamicNullObject)
                return DynamicNullObject.Null;

            throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a string, but was called with a parameter of type {attachmentName.GetType().FullName}: {attachmentName}");
        }

        public dynamic Id(dynamic doc)
        {
            if (doc is DynamicBlittableJson json)
                return json.GetId();

            if (doc is DynamicNullObject)
                return doc;

            ThrowInvalidDocType(doc, nameof(Id));

            // never hit
            return null;
        }

        public dynamic MetadataFor(dynamic doc)
        {
            if (doc is DynamicBlittableJson json)
            {
                json.EnsureMetadata();
                return doc[Constants.Documents.Metadata.Key];
            }

            if (doc is DynamicNullObject)
                return doc;

            ThrowInvalidDocType(doc, nameof(MetadataFor));

            // never hit
            return null;
        }

        public dynamic AsJson(dynamic doc)
        {
            if (doc is DynamicBlittableJson json)
            {
                json.EnsureMetadata();
                return json;
            }

            if (doc is DynamicNullObject)
                return doc;

            ThrowInvalidDocType(doc, nameof(AsJson));

            // never hit
            return null;
        }
        
        public dynamic AttachmentsFor(dynamic doc)
        {
            var metadata = MetadataFor(doc);
            var attachments = metadata is DynamicNullObject
                ? null : metadata[Constants.Documents.Metadata.Attachments];

            return attachments != null
                ? attachments
                : new DynamicArray(Enumerable.Empty<object>());
        }

        public dynamic CounterNamesFor(dynamic doc)
        {
            var metadata = MetadataFor(doc);
            var counters = metadata is DynamicNullObject
                ? null : metadata[Constants.Documents.Metadata.Counters];

            return counters != null
                ? counters
                : new DynamicArray(Enumerable.Empty<object>());
        }

        public dynamic TimeSeriesNamesFor(dynamic doc)
        {
            var metadata = MetadataFor(doc);
            var timeSeries = metadata is DynamicNullObject
                ? null : metadata[Constants.Documents.Metadata.TimeSeries];

            return timeSeries != null
                ? timeSeries
                : new DynamicArray(Enumerable.Empty<object>());
        }

        [DoesNotReturn]
        private static void ThrowInvalidDocType(dynamic doc, string funcName)
        {
            throw new InvalidOperationException(
                $"{funcName} may only be called with a document, " +
                $"but was called with a parameter of type {doc?.GetType().FullName}: {doc}");
        }
    }

    public abstract class AbstractStaticIndexBase
    {
        protected readonly Dictionary<string, CollectionName> _collectionsCache = new Dictionary<string, CollectionName>(StringComparer.OrdinalIgnoreCase);

        public readonly Dictionary<string, Dictionary<string, List<IndexingFunc>>> Maps = new Dictionary<string, Dictionary<string, List<IndexingFunc>>>();

        public readonly Dictionary<string, HashSet<CollectionName>> ReferencedCollections = new Dictionary<string, HashSet<CollectionName>>();

        public readonly HashSet<string> CollectionsWithCompareExchangeReferences = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        protected static RavenLogger Log = RavenLogManager.Instance.GetLoggerForServer<AbstractStaticIndexBase>();


        public int StackSizeInSelectClause { get; set; }
        
        public bool HasDynamicFields { get; set; }

        public bool HasBoostedFields { get; set; }

        public string Source;

        public IndexingFunc Reduce;

        public string[] OutputFields;

        public CompiledIndexField[] GroupByFields;

        private List<string> _groupByFieldNames;

        public List<string> GroupByFieldNames
        {
            get
            {
                return _groupByFieldNames ??= GroupByFields.Select(x => x.Name).ToList();
            }
        }

        public void AddCompareExchangeReferenceToCollection(string collection)
        {
            if (collection is null)
                throw new ArgumentNullException(nameof(collection));

            CollectionsWithCompareExchangeReferences.Add(collection);
        }

        public void AddReferencedCollection(string collection, string referencedCollection)
        {
            if (_collectionsCache.TryGetValue(referencedCollection, out CollectionName referencedCollectionName) == false)
                _collectionsCache[referencedCollection] = referencedCollectionName = new CollectionName(referencedCollection);

            if (ReferencedCollections.TryGetValue(collection, out HashSet<CollectionName> set) == false)
                ReferencedCollections[collection] = set = new HashSet<CollectionName>();

            set.Add(referencedCollectionName);
        }

        protected void AddMapInternal(string collection, string subCollecction, IndexingFunc map)
        {
            if (Maps.TryGetValue(collection, out Dictionary<string, List<IndexingFunc>> collections) == false)
                Maps[collection] = collections = new Dictionary<string, List<IndexingFunc>>();

            if (collections.TryGetValue(subCollecction, out var funcs) == false)
                collections[subCollecction] = funcs = new List<IndexingFunc>();

            funcs.Add(map);
        }

        internal void CheckDepthOfStackInOutputMap(IndexDefinition indexMetadata, DocumentDatabase documentDatabase)
        {
            var performanceHintConfig = documentDatabase.Configuration.PerformanceHints;
            if (StackSizeInSelectClause > performanceHintConfig.MaxDepthOfRecursionInLinqSelect)
            {
                documentDatabase.NotificationCenter.Add(PerformanceHint.Create(
                    documentDatabase.Name,
                    $"Index '{indexMetadata.Name}' contains {StackSizeInSelectClause} `let` clauses.",
                    $"We have detected that your index contains many `let` clauses. This can be not optimal approach because it might cause to allocate a lot of stack-based memory. Please consider to simplify your index definition. We suggest not to exceed {performanceHintConfig.MaxDepthOfRecursionInLinqSelect} `let` statements.",
                    PerformanceHintType.Indexing,
                    NotificationSeverity.Info,
                    nameof(IndexCompiler)));
                
                if (Log.IsWarnEnabled)
                    Log.Warn($"Index '{indexMetadata.Name}' contains a lot of `let` clauses. Stack size is {StackSizeInSelectClause}.");
            }
        }
        
        protected dynamic TryConvert<T>(object value)
            where T : struct
        {
            if (value == null || value is DynamicNullObject)
                return DynamicNullObject.Null;

            var type = typeof(T);
            if (type == typeof(double) || type == typeof(float))
            {
                var dbl = TryConvertToDouble(value);
                if (dbl.HasValue == false)
                    return DynamicNullObject.Null;

                if (type == typeof(float))
                    return (T)(object)Convert.ToSingle(dbl.Value);

                return (T)(object)dbl.Value;
            }

            if (type == typeof(long) || type == typeof(int))
            {
                var lng = TryConvertToLong(value);
                if (lng.HasValue == false)
                    return DynamicNullObject.Null;

                if (type == typeof(int))
                    return (T)(object)Convert.ToInt32(lng.Value);

                return (T)(object)lng.Value;
            }

            return DynamicNullObject.Null;

            static double? TryConvertToDouble(object v)
            {
                if (v is double d)
                    return d;
                if (v is LazyNumberValue lnv)
                    return lnv;
                if (v is int i)
                    return i;
                if (v is long l)
                    return l;
                if (v is float f)
                    return f;
                if (v is LazyStringValue lsv && double.TryParse(lsv, out var r))
                    return r;

                return null;
            }

            static long? TryConvertToLong(object v)
            {
                if (v is double d)
                    return (long)d;
                if (v is LazyNumberValue lnv)
                    return lnv;
                if (v is int i)
                    return i;
                if (v is long l)
                    return l;
                if (v is float f)
                    return (long)f;
                if (v is LazyStringValue lsv && long.TryParse(lsv, out var r))
                    return r;

                return null;
            }
        }

        public object CreateVector(string fieldName, object value)
        {
            var currentIndexingField = CurrentIndexingScope.Current.GetOrCreateVectorField(fieldName);
            PortableExceptions.ThrowIf<InvalidDataException>(currentIndexingField?.Vector is null,
                $"Field '{fieldName}' does not exist in this indexing scope. Cannot index as vector.");

            return CreateVector(currentIndexingField, value);
        }

        /// <summary>
        /// Create vector field object. This method is used by AutoIndexes.
        /// </summary>
        /// <param name="indexField">IndexField from IndexDefinition</param>
        /// <param name="value">Data source to create vector field.</param>
        /// <returns></returns>
        internal static object CreateVector(IndexField indexField, object value)
        {
#if DEBUG
            indexField.Vector.Validate();
#endif
            
            if (indexField!.Vector!.SourceEmbeddingType is EmbeddingType.Text)
                return VectorFromText(indexField, value);
            
            return VectorFromEmbedding(indexField, value);
        }

        private static object VectorFromEmbedding(IndexField currentIndexingField, object value)
        {
            var vectorOptions = currentIndexingField.Vector;
            PortableExceptions.ThrowIf<InvalidDataException>(value is DynamicNullObject or null, $"Cannot index data from a null value!");
            
            // We're expecting input in some-kind of numerical form (based on field's configuration). If not we can assume this is base64 form.
            if (value is LazyStringValue or LazyCompressedStringValue or string or DynamicNullObject)
                return GenerateEmbeddingForBase64(value.ToString());

            // if (value is BlittableJsonReaderArray blittableJsonReaderArray)
            // {
            //     return GenerateEmbeddingsFromDynamicArray(blittableJsonReaderArray);
            // }
            
            if (value is DynamicArray or BlittableJsonReaderArray)
            {
                var da = value as DynamicArray;
                var bjra = value as BlittableJsonReaderArray;
                object firstElement = (da?[0] ?? bjra?[0]);
                PortableExceptions.ThrowIf<InvalidDataException>(firstElement is null, $"There is no element in the vector!");
                
                var isEnumerableDynamicArray = firstElement is DynamicArray or BlittableJsonReaderArray; // should be safe since vector at least should have 1 dimension
                var isBase64 = firstElement is LazyStringValue or LazyCompressedStringValue or string or DynamicNullObject;

                // For float[][] we've to wrap it inside List<VectorValue>
                if (isEnumerableDynamicArray)
                {
                    var enumerable = (IEnumerable)da ?? (IEnumerable)bjra;
                    var vectorList = new List<VectorValue>();
                    foreach (var item in enumerable)
                        vectorList.Add(GenerateEmbeddingsFromDynamicArray((IEnumerable)item));
                    return vectorList;
                }
                
                // For string[] as well
                if (isBase64)
                {
                        var enumerable = (IEnumerable)da ?? (IEnumerable)bjra;
                        var vectorList = new List<VectorValue>();
                        foreach (var item in enumerable)
                            vectorList.Add(GenerateEmbeddingForBase64(item.ToString()));

                        return vectorList;
                }

                if (da != null)
                    return GenerateEmbeddingsFromDynamicArray(da);
                
                return GenerateEmbeddingsFromDynamicArray(bjra);
            }
            
            throw new InvalidDataException($"Not supported vector value: {value?.GetType().FullName}");

            VectorValue GenerateEmbeddingsFromDynamicArray<TEnumerable>(TEnumerable dynamicArray) where TEnumerable : IEnumerable
            {
                //Todo: we could use some kind of growable buffer instead list to avoid allocations
                IList embeddings = null;
                embeddings = vectorOptions.SourceEmbeddingType switch
                {
                    EmbeddingType.Float32 => new List<float>(),
                    EmbeddingType.Int8 => new List<sbyte>(),
                    EmbeddingType.Binary => new List<byte>(),
                    _ => throw new ArgumentOutOfRangeException()
                };

                foreach (var item in dynamicArray)
                {
                    object num = (item, vectorOptions.SourceEmbeddingType) switch
                    {
                        (LazyNumberValue lNV, EmbeddingType.Float32) => Convert.ToSingle((float)lNV),
                        (LazyNumberValue lNV, EmbeddingType.Int8) => Convert.ToSByte((sbyte)lNV),
                        (float, EmbeddingType.Float32) => Convert.ToSingle(item),
                        (double d, EmbeddingType.Float32) => Convert.ToSingle(d),
                        (long l, EmbeddingType.Float32) => Convert.ToSingle(l),
                        (int i, EmbeddingType.Float32) => Convert.ToSingle(i),
                        (_, EmbeddingType.Binary) => Convert.ToByte(item),
                        (long l, EmbeddingType.Int8) => Convert.ToSByte(l),
                        (int i, EmbeddingType.Int8) => Convert.ToSByte(i),
                        
                        _ => throw new NotSupportedException($"Vector.Search() on ({item.GetType().FullName}, {vectorOptions.SourceEmbeddingType}) is not supported.")
                    };

                    switch (vectorOptions.SourceEmbeddingType)
                    {
                        case EmbeddingType.Float32:
                            embeddings.Add((float)num);
                            break;
                        case EmbeddingType.Int8:
                            embeddings.Add(Convert.ToSByte(num));
                            break;
                        default:
                            embeddings.Add((byte)num);
                            break;
                    }
                }

                return embeddings switch
                {
                    List<float> vectorOfSingles => GenerateEmbeddings.FromArray(vectorOptions.SourceEmbeddingType, vectorOptions.DestinationEmbeddingType, vectorOfSingles.ToArray()),
                    List<byte> vectorOfBytes => GenerateEmbeddings.FromArray(vectorOptions.SourceEmbeddingType, vectorOptions.DestinationEmbeddingType, vectorOfBytes.ToArray()),
                    List<sbyte> vectorOfSbytes => GenerateEmbeddings.FromArray(vectorOptions.SourceEmbeddingType, vectorOptions.DestinationEmbeddingType, vectorOfSbytes.ToArray()),
                    _ => throw new InvalidDataException($"Vector values must be either float, byte, or sbyte. Received `{embeddings.GetType().FullName}`.")
                };
            }

            VectorValue GenerateEmbeddingForBase64(string source)
            {
                var decodedBase = Convert.FromBase64String(source);
                if (vectorOptions.SourceEmbeddingType is EmbeddingType.Float32)
                {
                    var embeddings = MemoryMarshal.Cast<byte, float>(decodedBase);
                    return GenerateEmbeddings.FromArray(vectorOptions.SourceEmbeddingType, vectorOptions.DestinationEmbeddingType, embeddings.ToArray());
                }

                return GenerateEmbeddings.FromArray(vectorOptions.SourceEmbeddingType, vectorOptions.DestinationEmbeddingType, decodedBase);
            }
        }

        private static object VectorFromText(IndexField indexField, object value)
        {
            object embedding = null;

            if (value is LazyStringValue or LazyCompressedStringValue or string or DynamicNullObject)
                embedding = CreateVectorValue(value);
            else if (value is IEnumerable enumerable)
            {
                var vectorList = new List<VectorValue>();
                foreach (var item in enumerable)
                    vectorList.Add(CreateVectorValue(item));
                embedding = vectorList;
            }

            PortableExceptions.ThrowIf<InvalidDataException>(embedding is null, $"Tried to convert text into embeddings but got type {value?.GetType().FullName} which is not supported.");
            return embedding;

            VectorValue CreateVectorValue(object value)
            {
                var str = value switch
                {
                    LazyStringValue lsv => (string)lsv,
                    LazyCompressedStringValue lcsv => lcsv,
                    string s => s,
                    DynamicNullObject => null,
                    null => null,
                    _ => throw new NotSupportedException("Only strings are supported, but got: " + value?.GetType().FullName)
                };

                if (str == null)
                    throw new InvalidDataException($"Unsupported vector value type: {value?.GetType().FullName}");

                return GenerateEmbeddings.FromText(indexField.Vector.DestinationEmbeddingType, str);
            }
        }

        public dynamic LoadDocument<TIgnored>(object keyOrEnumerable, string collectionName)
        {
            return LoadDocument(keyOrEnumerable, collectionName);
        }

        public dynamic LoadDocument(object keyOrEnumerable, string collectionName)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException(
                    "Indexing scope was not initialized. Key: " + keyOrEnumerable);

            if (keyOrEnumerable is LazyStringValue keyLazy)
                return CurrentIndexingScope.Current.LoadDocument(keyLazy, null, collectionName);

            if (keyOrEnumerable is string keyString)
                return CurrentIndexingScope.Current.LoadDocument(null, keyString, collectionName);

            if (keyOrEnumerable is DynamicNullObject || keyOrEnumerable is null)
                return DynamicNullObject.Null;

            if (keyOrEnumerable is IEnumerable enumerable)
            {
                var enumerator = enumerable.GetEnumerator();
                using (enumerable as IDisposable)
                {
                    var items = new List<dynamic>();
                    while (enumerator.MoveNext())
                    {
                        items.Add(LoadDocument(enumerator.Current, collectionName));
                    }
                    if (items.Count == 0)
                        return DynamicNullObject.Null;

                    return new DynamicArray(items);
                }
            }

            throw new InvalidOperationException(
                "LoadDocument may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        public dynamic LoadCompareExchangeValue<TIgnored>(object keyOrEnumerable)
        {
            return LoadCompareExchangeValue(keyOrEnumerable);
        }

        public dynamic LoadCompareExchangeValue(object keyOrEnumerable)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException(
                    "Indexing scope was not initialized. Key: " + keyOrEnumerable);

            if (keyOrEnumerable is LazyStringValue keyLazy)
                return CurrentIndexingScope.Current.LoadCompareExchangeValue(keyLazy, null);

            if (keyOrEnumerable is string keyString)
                return CurrentIndexingScope.Current.LoadCompareExchangeValue(null, keyString);

            if (keyOrEnumerable is DynamicNullObject)
                return DynamicNullObject.Null;

            if (keyOrEnumerable is IEnumerable enumerable)
            {
                var enumerator = enumerable.GetEnumerator();
                using (enumerable as IDisposable)
                {
                    var items = new List<dynamic>();
                    while (enumerator.MoveNext())
                    {
                        items.Add(LoadCompareExchangeValue(enumerator.Current));
                    }
                    if (items.Count == 0)
                        return DynamicNullObject.Null;

                    return new DynamicArray(items);
                }
            }

            throw new InvalidOperationException(
                "LoadCompareExchangeValue may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        public IEnumerable<dynamic> Recurse(object item, Func<dynamic, dynamic> func)
        {
            return new RecursiveFunction(item, func).Execute();
        }

        protected IEnumerable<object> CreateField(string name, object value, CreateFieldOptions options)
        {
            if (CurrentIndexingScope.Current.SupportsDynamicFieldsCreation == false)
                return null;

            var scope = CurrentIndexingScope.Current;
            return scope.Index.SearchEngineType switch
            {
                SearchEngineType.Corax => CoraxCreateField(scope, name, value, options),
                _ => LuceneCreateField(scope, name, value, options)
            };
        }

        protected IEnumerable<CoraxDynamicItem> CoraxCreateField(CurrentIndexingScope scope, string name, object value, CreateFieldOptions options)
        {
            IndexFieldOptions allFields = null;
            if (scope.IndexDefinition is MapIndexDefinition mapIndexDefinition)
            {
                mapIndexDefinition.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out allFields);
            }

            var field = IndexField.Create(name, new IndexFieldOptions { Storage = options?.Storage, TermVector = options?.TermVector, Indexing = options?.Indexing, },
                allFields, Corax.Constants.IndexWriter.DynamicField);

            scope.DynamicFields ??= new Dictionary<string, IndexField>();
            if (scope.DynamicFields.TryGetValue(name, out var existing) == false)
            {
                scope.DynamicFields[name] = field;
                scope.IncrementDynamicFields();
            }
            else if (options?.Indexing != null && existing.Indexing != field.Indexing)
            {
                throw new InvalidDataException($"Inconsistent dynamic field creation options were detected. Field '{name}' was created with '{existing.Indexing}' analyzer but now '{field.Indexing}' analyzer was specified. This is not supported");
            }


            var result = new List<CoraxDynamicItem>
            {
                new()
                {
                    Field = field,
                    FieldName = name,
                    Value = value
                }
            };

            return result;
        }
        
        private IEnumerable<AbstractField> LuceneCreateField(CurrentIndexingScope scope, string name, object value, CreateFieldOptions options)
        {
            // IMPORTANT: Do not delete this method, it is used by the indexes code when using CreateField

            options = options ?? CreateFieldOptions.Default;

            IndexFieldOptions allFields = null;
            if (scope.IndexDefinition is MapIndexDefinition mapIndexDefinition)
                mapIndexDefinition.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out allFields);

            var field = IndexField.Create(name, new IndexFieldOptions
            {
                Storage = options.Storage,
                TermVector = options.TermVector,
                Indexing = options.Indexing
            }, allFields);

            if (scope.DynamicFields == null)
                scope.DynamicFields = new Dictionary<string, IndexField>();

            scope.DynamicFields[name] = field;

            if (scope.CreateFieldConverter == null)
                scope.CreateFieldConverter = new LuceneDocumentConverter(scope.Index, new IndexField[] { });

            using var i = scope.CreateFieldConverter.NestedField(scope.CreatedFieldsCount);
            scope.IncrementDynamicFields();
            var result = new List<AbstractField>();
            scope.CreateFieldConverter.GetRegularFields(new StaticIndexLuceneDocumentWrapper(result), field, value, CurrentIndexingScope.Current.IndexContext, scope?.Source, out _);
            return result;
        }
        
        protected IEnumerable<object> CreateField(string name, object value, bool stored = false, bool? analyzed = null)
        {
            if (CurrentIndexingScope.Current.SupportsDynamicFieldsCreation == false)
                return null;
            // IMPORTANT: Do not delete this method, it is used by the indexes code when using CreateField

            FieldIndexing? indexing;

            switch (analyzed)
            {
                case true:
                    indexing = FieldIndexing.Search;
                    break;
                case false:
                    indexing = FieldIndexing.Exact;
                    break;
                default:
                    indexing = null;
                    break;
            }

            var scope = CurrentIndexingScope.Current;
            var creationFieldOptions = new CreateFieldOptions
            {
                Storage = stored ? FieldStorage.Yes : FieldStorage.No, Indexing = indexing, TermVector = FieldTermVector.No
            };
            return scope.Index.SearchEngineType switch
            {
                SearchEngineType.Corax => CoraxCreateField(scope, name, value, creationFieldOptions),
                _ => LuceneCreateField(scope, name, value, creationFieldOptions)
            };
        }

        public unsafe dynamic AsDateOnly(dynamic field)
        {
            if (field is LazyStringValue lsv)
            {
                if (LazyStringParser.TryParseDateOnly(lsv.Buffer, lsv.Length, out var @do) == false) 
                    return DynamicNullObject.Null;
                
                return @do;
            }
            
            if (field is string str)
            {
                fixed (char* strBuffer = str.AsSpan())
                {
                    if (LazyStringParser.TryParseDateOnly(strBuffer, str.Length, out var to) == false)
                        return DynamicNullObject.Null;

                    return to;
                }
            }
            
            if (field is DateTime dt)
            {
                return DateOnly.FromDateTime(dt);
            }

            if (field is DateOnly dtO)
            {
                return dtO;
            }
            
            if (field is null)
            {
                return DynamicNullObject.ExplicitNull;
            }
            
            if (field is DynamicNullObject dno)
            {
                return dno;
            }
            
            throw new InvalidDataException($"Expected {nameof(DateTime)}, {nameof(DateOnly)}, null, string or JSON value.");
        }

        public unsafe dynamic AsTimeOnly(dynamic field)
        {
            if (field is LazyStringValue lsv)
            {
                if (LazyStringParser.TryParseTimeOnly(lsv.Buffer, lsv.Length, out var to) == false)
                    return DynamicNullObject.Null;

                return to;
            }

            if (field is string str)
            {
                fixed (char* strBuffer = str.AsSpan())
                {
                    if (LazyStringParser.TryParseTimeOnly(strBuffer, str.Length, out var to) == false)
                        return DynamicNullObject.Null;

                    return to;
                }
            }
            
            if (field is TimeSpan ts)
            {
                return TimeOnly.FromTimeSpan(ts);
            }

            if (field is TimeOnly toF)
            {
                return toF;
            }

            if (field is null)
            {
                return DynamicNullObject.ExplicitNull;
            }
            
            if (field is DynamicNullObject dno)
            {
                return dno;
            }
            
            throw new InvalidDataException($"Expected {nameof(TimeSpan)}, {nameof(TimeOnly)}, null, string or JSON value.");
        }

        public IEnumerable<object> CreateSpatialField(string name, object lat, object lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            return CreateSpatialField(name, ConvertToDouble(lat), ConvertToDouble(lng));
        }

        public IEnumerable<object> CreateSpatialField(string name, double? lat, double? lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            var spatialField = GetOrCreateSpatialField(name);

            return CreateSpatialField(spatialField, lat, lng);
        }

        public IEnumerable<object> CreateSpatialField(string name, object shapeWkt)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            var spatialField = GetOrCreateSpatialField(name);
            return CreateSpatialField(spatialField, shapeWkt);
        }

        internal static IEnumerable<object> CreateSpatialField(SpatialField spatialField, object lat, object lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            return CreateSpatialField(spatialField, ConvertToDouble(lat), ConvertToDouble(lng));
        }

        internal static IEnumerable<object> CreateSpatialField(SpatialField spatialField, double? lat, double? lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");

            if (CurrentIndexingScope.Current.SupportsSpatialFieldsCreation == false)
                return null;

            if (lng == null || double.IsNaN(lng.Value))
                return Enumerable.Empty<AbstractField>();
            if (lat == null || double.IsNaN(lat.Value))
                return Enumerable.Empty<AbstractField>();

            IShape shape = spatialField.GetContext().MakePoint(lng.Value, lat.Value);
            return CurrentIndexingScope.Current.Index.SearchEngineType is SearchEngineType.Lucene
                ? spatialField.LuceneCreateIndexableFields(shape)
                : spatialField.CoraxCreateIndexableFields(shape);
        }

        internal static IEnumerable<object> CreateSpatialField(SpatialField spatialField, object shapeWkt)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");

            if (CurrentIndexingScope.Current.SupportsSpatialFieldsCreation == false)
                return null;

            return CurrentIndexingScope.Current.Index.SearchEngineType is SearchEngineType.Lucene
                ? spatialField.LuceneCreateIndexableFields(shapeWkt)
                : spatialField.CoraxCreateIndexableFields(shapeWkt);
        }

        internal static SpatialField GetOrCreateSpatialField(string name)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");

            if (CurrentIndexingScope.Current.SupportsSpatialFieldsCreation == false)
                return null;

            return CurrentIndexingScope.Current.GetOrCreateSpatialField(name);
        }

        private static double? ConvertToDouble(object value)
        {
            if (value == null || value is DynamicNullObject)
                return null;

            if (value is LazyNumberValue lnv)
                return lnv.ToDouble(CultureInfo.InvariantCulture);

            return Convert.ToDouble(value);
        }

        internal struct StaticIndexLuceneDocumentWrapper : ILuceneDocumentWrapper
        {
            private readonly List<AbstractField> _fields;

            public StaticIndexLuceneDocumentWrapper(List<AbstractField> fields)
            {
                _fields = fields;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Add(AbstractField field)
            {
                _fields.Add(field);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IList<IFieldable> GetFields()
            {
                throw new NotImplementedException();
            }
        }
    }
}
