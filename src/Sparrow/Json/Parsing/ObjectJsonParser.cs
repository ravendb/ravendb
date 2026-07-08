using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using Sparrow.Collections;
using Sparrow.Extensions;
using Sparrow.Utils;

namespace Sparrow.Json.Parsing
{
    public interface IDynamicJson
    {
        DynamicJsonValue ToJson();
    }

    public sealed class DynamicJsonValue
    {
        public const string TypeFieldName = "$type";

        public int SourceIndex = -1;
        public BlittableJsonReaderObject.InsertionOrderProperties SourceProperties;

        public int ModificationsIndex = 0;
        public readonly List<(string Name, object Value)> Properties = new();
        public HashSet<int> Removals;
        internal readonly BlittableJsonReaderObject _source;

        public DynamicJsonValue()
        {
        }

        public DynamicJsonValue(string key, object value)
        {
            Properties.Add((key, value));
        }

        public DynamicJsonValue(Type explicitTypeInfo)
        {
            this[TypeFieldName] = explicitTypeInfo.GetTypeNameForSerialization();
        }

        public DynamicJsonValue(BlittableJsonReaderObject source)
        {
            _source = source;

            if (_source != null)
            {
#if DEBUG
                if (_source.Modifications != null && _source.Modifications.Properties.Count != _source.Modifications.ModificationsIndex)
                    throw new InvalidOperationException("The source already has modifications");
#endif
                _source.Modifications = this;
            }
        }

        public void Remove(string property)
        {
            if (_source == null)
                throw new InvalidOperationException(
                    "Cannot remove property when not setup with a source blittable json object");

            var propertyIndex = _source.GetPropertyIndex(property);
            if (propertyIndex == -1)
                return;

            Removals ??= new HashSet<int>();
            Removals.Add(propertyIndex);
            for (int i = 0; i < Properties.Count; i++)
            {
                if (Properties[i].Name == property)
                {
                    Properties.RemoveAt(i);
                    break;
                }
            }
        }

        internal void RemoveInMemoryPropertyByName(string property)
        {
            if (_source != null)
                throw new InvalidOperationException(
                    "Cannot remove in memory property when setup with a source blittable json object");
            var index = Properties.FindIndex(x => x.Name == property);
            if (index == -1) return;
            Properties.RemoveAt(index);
        }

        public object this[string name]
        {
            set
            {
#if DEBUG
                if (value != null &&
                    value.GetType().FullName == "Raven.Server.Documents.Document")
                {
                    throw new InvalidOperationException("Cannot add Document to DynamicJsonValue");
                }
#endif
                if (_source != null)
                    Remove(name);
                Properties.Add((name, value));
            }
            get
            {
                foreach (var property in Properties)
                {
                    if (property.Item1 != name)
                        continue;

                    return property.Item2;
                }

                return null;
            }
        }

        public static DynamicJsonValue Convert<T>(IDictionary<string, T> dictionary)
        {
            if (dictionary == null)
                return null;

            var djv = new DynamicJsonValue();
            foreach (var kvp in dictionary)
            {
                var json = kvp.Value as IDynamicJson;
                djv[kvp.Key] = json == null ? (object)kvp.Value : json.ToJson();
            }
            return djv;
        }

        public static DynamicJsonValue Convert<TK, TV>(IDictionary<TK, TV> dictionary)
        {
            if (dictionary == null)
                return null;

            // if (typeof(TK).IsPrimitive == false)
            {
                var mi = typeof(TK).GetMethod(nameof(ToString), types: Type.EmptyTypes);
                if (mi.GetBaseDefinition().DeclaringType == mi.DeclaringType)
                    throw new InvalidOperationException($"{typeof(TK).FullName} must override 'ToString'");
            }

            var djv = new DynamicJsonValue();
            foreach (var kvp in dictionary)
            {
                var json = kvp.Value as IDynamicJson;
                djv[kvp.Key.ToString()] = json == null ? (object)kvp.Value : json.ToJson();
            }
            return djv;
        }
    }

    public sealed class DynamicJsonArray : IEnumerable<object>, IDisposable
    {
        public bool SkipOriginalArray;
        public int SourceIndex = -1;
        public int ModificationsIndex;
        public readonly List<object> Items;
        public List<int> Removals;

        public DynamicJsonArray()
        {
            Items = new List<object>();
        }

        public DynamicJsonArray(IEnumerable collection)
        {
            Items = new List<object>();
            foreach (object o in collection)
            {
                Items.Add(o);
            }
        }

        public void RemoveAt(int index)
        {
            Removals ??= new List<int>();
            Removals.Add(index);
        }

        public void Add(object obj)
        {
            EnsureNotDocumentInArray(obj);
            Items.Add(obj);
        }

        [Conditional("DEBUG")]
        private static void EnsureNotDocumentInArray(object value)
        {
            if (value != null &&
                value.GetType().FullName == "Raven.Server.Documents.Document")
            {
                throw new InvalidOperationException("Cannot add Document to DynamicJsonArray");
            }
        }

        public int Count => Items.Count;

        public IEnumerator<object> GetEnumerator()
        {
            return Items.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Clear()
        {
            Items.Clear();
        }

        public void Dispose()
        {
            foreach (var item in Items)
            {
                if (item is IDisposable toDispose)
                    toDispose.Dispose();
            }
        }
    }

    public sealed unsafe class ObjectJsonParser(JsonParserState state, JsonOperationContext ctx) : IJsonParser
    {
        private readonly JsonParserState _state = state;
        private readonly JsonOperationContext _ctx = ctx;
        private readonly FastStack<object> _elements = new FastStack<object>();

        private bool _disposed;
        private AllocatedMemoryData _currentStateBuffer;

        private readonly HashSet<object> _seenValues = new HashSet<object>(ReferenceEqualityComparer<object>.Default);

        public void Reset(object root)
        {
            if (_currentStateBuffer != null)
            {
                _ctx.ReturnMemory(_currentStateBuffer);
                _currentStateBuffer = null;
            }

            _elements.WeakClear();
            _seenValues.Clear();

            if (root != null)
                _elements.Push(root);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            if (_currentStateBuffer != null)
                _ctx.ReturnMemory(_currentStateBuffer);
        }


        private enum KnownJsonObjectType : byte
        {
            // We introduced a dedicated enum to represent each recognized data shape.
            // This replaced the repeated, nested 'if (current is X) { ... } else if ...' pattern,
            // making the parse loop both smaller and easier for the JIT to optimize.

            // Primary object flows  
            DynamicJsonValue, // DynamicJsonValue => goto ValueTuple
            ValueTuple,

            // Simple numeric / boolean
            // (all are quite common and return immediately)
            Int64,                    // (covers long)
            Int32,                    // (covers int)
            Int16OrLesser,            // (covers short, byte, sbyte, ushort)
            UInt32,                   // (covers uint)
            UInt64,                   // (covers ulong)
            Boolean,

            IDynamicJson, // IDynamicJson => goto DynamicJsonValue             
            BlittableJsonReaderObject, // BlittableJsonReaderObject => goto DynamicJsonValue

            // Lazy string forms
            LazyStringValue,
            LazyCompressedStringValue,
            LazyNumberValue,

            // Character-like => goto handle string
            Char,
            EnumType,
            String,

            // Special handle cases for string / number
            HandleSetStringBufferOptimization,
            HandleSetStringBufferNumberOptimization,

            Float, // Float => goto handle number
            Double, // Double => goto handle number
            Decimal, // Decimal => can become long or string

#if NET6_0_OR_GREATER
            Half, // Half => goto handle number
#endif

            // All datetime based => goto handle string
            DateTime,
            DateTimeOffset,
            TimeSpan,
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
            TimeOnly,
            DateOnly,
#endif

            // Collections => goto DynamicJsonArray
            ListOfLong,
            ListOfDouble,
            DictionaryStringLong,

            IEnumerableObject, // IEnumerableObject => goto DynamicJsonArray

            BlittableJsonReaderArray, // BlittableJsonReaderArray => goto DynamicJsonArray
            DynamicJsonArray, // DynamicJsonArray => base array type
            BlittableJsonReaderVector, // BlittableJsonReaderVector => goto DynamicJsonArray

            IBlittableJsonContainer, // IBlittableJsonContainer => goto BlittableJsonReaderObject
            RawBlob,
        }

        private static readonly ReplacementTypeCache<KnownJsonObjectType> TypeCache = new(4096);

        private KnownJsonObjectType GetKnownJsonObjectTypeUnlikely(object current)
        {
            var type = current.GetType();
            if (current is IDynamicJson idj)
            {
                TypeCache.Put(type, KnownJsonObjectType.IDynamicJson);
                return KnownJsonObjectType.IDynamicJson;
            }

            if (current is DynamicJsonValue value)
            {
                TypeCache.Put(type, KnownJsonObjectType.DynamicJsonValue);
                return KnownJsonObjectType.DynamicJsonValue;
            }

            if (current is DynamicJsonArray array)
            {
                TypeCache.Put(type, KnownJsonObjectType.DynamicJsonArray);
                return KnownJsonObjectType.DynamicJsonArray;
            }

            if (current is ValueTuple<string, object> vt)
            {
                TypeCache.Put(type, KnownJsonObjectType.ValueTuple);
                return KnownJsonObjectType.ValueTuple;
            }

            if (current is BlittableJsonReaderObject bjro)
            {
                TypeCache.Put(type, KnownJsonObjectType.BlittableJsonReaderObject);
                return KnownJsonObjectType.BlittableJsonReaderObject;
            }

            if (current is BlittableJsonReaderArray bjra)
            {
                TypeCache.Put(type, KnownJsonObjectType.BlittableJsonReaderArray);
                return KnownJsonObjectType.BlittableJsonReaderArray;
            }

            if (current is BlittableJsonReaderVector bjrv)
            {
                TypeCache.Put(type, KnownJsonObjectType.BlittableJsonReaderVector);
                return KnownJsonObjectType.BlittableJsonReaderVector;
            }

            if (current is IBlittableJsonContainer dbj)
            {
                TypeCache.Put(type, KnownJsonObjectType.IBlittableJsonContainer);
                return KnownJsonObjectType.IBlittableJsonContainer;
            }

            if (current is IEnumerable<object> enumerable)
            {
                TypeCache.Put(type, KnownJsonObjectType.IEnumerableObject);
                return KnownJsonObjectType.IEnumerableObject;
            }

            if (current is LazyStringValue lsv)
            {
                TypeCache.Put(type, KnownJsonObjectType.LazyStringValue);
                return KnownJsonObjectType.LazyStringValue;
            }

            if (current is BlittableJsonReaderObject.RawBlob bs)
            {
                TypeCache.Put(type, KnownJsonObjectType.RawBlob);
                return KnownJsonObjectType.RawBlob;
            }

            if (current is LazyCompressedStringValue lcsv)
            {
                TypeCache.Put(type, KnownJsonObjectType.LazyCompressedStringValue);
                return KnownJsonObjectType.LazyCompressedStringValue;
            }

            if (current is LazyNumberValue ldv)
            {
                TypeCache.Put(type, KnownJsonObjectType.LazyNumberValue);
                return KnownJsonObjectType.LazyNumberValue;
            }

            if (current is string)
            {
                TypeCache.Put(type, KnownJsonObjectType.String);
                return KnownJsonObjectType.String;
            }

            if (current is char)
            {
                TypeCache.Put(type, KnownJsonObjectType.Char);
                return KnownJsonObjectType.Char;
            }

            if (current is byte || current is sbyte || current is short || current is ushort)
            {
                TypeCache.Put(type, KnownJsonObjectType.Int16OrLesser);
                return KnownJsonObjectType.Int16OrLesser;
            }

            if (current is int)
            {
                TypeCache.Put(type, KnownJsonObjectType.Int32);
                return KnownJsonObjectType.Int32;
            }

            if (current is long)
            {
                TypeCache.Put(type, KnownJsonObjectType.Int64);
                return KnownJsonObjectType.Int64;
            }

            if (current is uint)
            {
                TypeCache.Put(type, KnownJsonObjectType.UInt32);
                return KnownJsonObjectType.UInt32;
            }

            if (current is ulong)
            {
                TypeCache.Put(type, KnownJsonObjectType.UInt64);
                return KnownJsonObjectType.UInt64;
            }

            if (current is bool)
            {
                TypeCache.Put(type, KnownJsonObjectType.Boolean);
                return KnownJsonObjectType.Boolean;
            }

            if (current is float)
            {
                TypeCache.Put(type, KnownJsonObjectType.Float);
                return KnownJsonObjectType.Float;
            }

            if (current is double)
            {
                TypeCache.Put(type, KnownJsonObjectType.Double);
                return KnownJsonObjectType.Double;
            }

#if NET6_0_OR_GREATER
            if (current is Half)
            {
                TypeCache.Put(type, KnownJsonObjectType.Half);
                return KnownJsonObjectType.Half;
            }
#endif

            if (current is DateTime)
            {
                TypeCache.Put(type, KnownJsonObjectType.DateTime);
                return KnownJsonObjectType.DateTime;
            }

            if (current is DateTimeOffset)
            {
                TypeCache.Put(type, KnownJsonObjectType.DateTimeOffset);
                return KnownJsonObjectType.DateTimeOffset;
            }

            if (current is TimeSpan)
            {
                TypeCache.Put(type, KnownJsonObjectType.TimeSpan);
                return KnownJsonObjectType.TimeSpan;
            }

#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
            if (current is TimeOnly)
            {
                TypeCache.Put(type, KnownJsonObjectType.TimeOnly);
                return KnownJsonObjectType.TimeOnly;
            }

            if (current is DateOnly)
            {
                TypeCache.Put(type, KnownJsonObjectType.DateOnly);
                return KnownJsonObjectType.DateOnly;
            }
#endif

            if (current is decimal)
            {
                TypeCache.Put(type, KnownJsonObjectType.Decimal);
                return KnownJsonObjectType.Decimal;
            }

            if (current is List<long>)
            {
                TypeCache.Put(type, KnownJsonObjectType.ListOfLong);
                return KnownJsonObjectType.ListOfLong;
            }

            if (current is List<double>)
            {
                TypeCache.Put(type, KnownJsonObjectType.ListOfDouble);
                return KnownJsonObjectType.ListOfDouble;
            }

            if (current is Dictionary<string, long>)
            {
                TypeCache.Put(type, KnownJsonObjectType.DictionaryStringLong);
                return KnownJsonObjectType.DictionaryStringLong;
            }

            if (current is Enum)
            {
                TypeCache.Put(type, KnownJsonObjectType.EnumType);
                return KnownJsonObjectType.EnumType;
            }

            throw new InvalidOperationException("Got unknown type: " + current.GetType() + " " + current);
        }

        public bool Read()
        {
            if (_disposed)
                ThrowOnDisposed();

            if (_elements.Count == 0)
                throw new EndOfStreamException();

            var current = _elements.Pop();

            var state = _state;

            string auxiliaryString = null;
            double auxiliaryDouble = 0;
            JsonParserToken auxiliaryToken = JsonParserToken.None;

            while (true)
            {
                if (current == null)
                {
                    state.CurrentTokenType = JsonParserToken.Null;
                    return true;
                }

                // PERF: The central optimization is the "TypeCache.TryGet(...)"
                // call, which finds or assigns a known classification for the current
                // object type. This drastically reduces the overhead of the prior
                // multi-level if/else chain. Repeatedly encountered types are recognized
                // in O(1) time, letting the parser jump directly to the relevant handling block.
                if (TypeCache.TryGet(current.GetType(), out var knownType) == false)
                {
                    // If not in cache, determine and cache it
                    knownType = GetKnownJsonObjectTypeUnlikely(current);
                }

                switch (knownType)
                {
                    case KnownJsonObjectType.DynamicJsonValue:
                    {
                        // PERF: We handle the normal logic for 'DynamicJsonValue' here (which is the most common),
                        // same as before. The difference is that we get here quickly as 0 is no jump 
                        // thanks to the TypeCache classification.
                        var value = (DynamicJsonValue)current;
                        if (_seenValues.Add(value))
                        {
#if DEBUG
                            if (value._source != null)
                                throw new InvalidOperationException("Trying to directly modify a DynamicJsonValue with a source, but you need to place the source (blittable), not the json value in the parent.");
#endif
                            state.CurrentTokenType = JsonParserToken.StartObject;
                            value.SourceIndex = -1;
                            value.ModificationsIndex = 0;
                            _elements.Push(value);
                            return true;
                        }
                        if (value.ModificationsIndex >= value.Properties.Count)
                        {
                            _seenValues.Remove(value);
                            state.CurrentTokenType = JsonParserToken.EndObject;
                            return true;
                        }
                        _elements.Push(value);
                        current = value.Properties[value.ModificationsIndex++];
                        if (current == null)
                            continue;

                        // PERF: We know we are dealing with a ValueTuple so we just continue to it.
                        goto case KnownJsonObjectType.ValueTuple;
                    }
                    case KnownJsonObjectType.ValueTuple:
                    {
                        var vt = (ValueTuple<string, object>)current;
                        _elements.Push(vt.Item2);
                        current = vt.Item1;
                        continue;
                    }
                    case KnownJsonObjectType.Int64:
                    {
                        state.Long = (long)current;
                        state.CurrentTokenType = JsonParserToken.Integer;
                        return true;
                    }
                    case KnownJsonObjectType.Int32:
                    {
                        state.Long = (int)current;
                        state.CurrentTokenType = JsonParserToken.Integer;
                        return true;
                    }
                    case KnownJsonObjectType.Int16OrLesser:
                    {
                        state.Long = Convert.ToInt64(current);
                        state.CurrentTokenType = JsonParserToken.Integer;
                        return true;
                    }
                    case KnownJsonObjectType.UInt32:
                    {
                        state.Long = (uint)current;
                        state.CurrentTokenType = JsonParserToken.Integer;
                        return true;
                    }
                    case KnownJsonObjectType.UInt64:
                    {
                        state.Long = (long)(ulong)current;
                        state.CurrentTokenType = JsonParserToken.Integer;
                        return true;
                    }


                    case KnownJsonObjectType.Boolean:
                    {
                        state.CurrentTokenType = (bool)current ? JsonParserToken.True : JsonParserToken.False;
                        return true;
                    }
                    case KnownJsonObjectType.IDynamicJson:
                    {
                        var idj = (IDynamicJson)current;
                        current = idj.ToJson();
                        goto case KnownJsonObjectType.DynamicJsonValue; // We know where it must go.
                    }
                    case KnownJsonObjectType.BlittableJsonReaderObject:
                    {
                        var bjro = (BlittableJsonReaderObject)current;
                        bjro.Modifications ??= new DynamicJsonValue(bjro);

                        var modifications = bjro.Modifications;

                        if (_seenValues.Add(modifications))
                        {
                            _elements.Push(bjro);
                            modifications.SourceIndex = -1;
                            modifications.ModificationsIndex = 0;
                            modifications.SourceProperties = bjro.GetPropertiesByInsertionOrder();
                            state.CurrentTokenType = JsonParserToken.StartObject;
                            return true;
                        }

                        modifications.SourceIndex++;
                        var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                        if (modifications.SourceIndex < modifications.SourceProperties.Size)
                        {
                            var propIndex = modifications.SourceProperties.Properties[modifications.SourceIndex];
                            if (modifications.Removals != null && modifications.Removals.Contains(propIndex))
                                continue;

                            bjro.GetPropertyByIndex(propIndex, ref propDetails);
                            _elements.Push(bjro);
                            _elements.Push(propDetails.Value);
                            current = propDetails.Name;
                            continue;
                        }
                        modifications.SourceProperties.Dispose();
                        current = modifications;
                        goto case KnownJsonObjectType.DynamicJsonValue;
                    }


                    case KnownJsonObjectType.LazyStringValue:
                    {
                        var lsv = (LazyStringValue)current;
                        state.StringBuffer = lsv.Buffer;
                        state.StringSize = lsv.Size;
                        state.CompressedSize = null;// don't even try
                        state.CurrentTokenType = JsonParserToken.String;
                        ReadEscapePositions(lsv.Buffer, lsv.Size);
                        return true;
                    }
                    case KnownJsonObjectType.LazyCompressedStringValue:
                    {
                        var lcsv = (LazyCompressedStringValue)current;
                        state.StringBuffer = lcsv.Buffer;
                        state.StringSize = lcsv.UncompressedSize;
                        state.CompressedSize = lcsv.CompressedSize;
                        state.CurrentTokenType = JsonParserToken.String;
                        ReadEscapePositions(lcsv.Buffer, lcsv.CompressedSize);
                        return true;
                    }
                    case KnownJsonObjectType.LazyNumberValue:
                    {
                        // RavenDB-22076: Notice here we are forcing the token type to Float, this is not correct for
                        // proper handling when we are dealing with Vector so we will need to pay the cost to adjust
                        // at the level of vector handling because of compatibility concerns. 
                        var ldv = (LazyNumberValue)current;
                        state.StringBuffer = ldv.Inner.Buffer;
                        state.StringSize = ldv.Inner.Size;
                        state.CompressedSize = null;// don't even try
                        state.CurrentTokenType = JsonParserToken.Float;
                        ReadEscapePositions(ldv.Inner.Buffer, ldv.Inner.Size);
                        return true;
                    }

                    case KnownJsonObjectType.Char:
                    {
                        auxiliaryString = new string((char)current, 1);
                        auxiliaryToken = JsonParserToken.String;
                        goto case KnownJsonObjectType.HandleSetStringBufferOptimization;
                    }
                    case KnownJsonObjectType.EnumType:
                    {
                        auxiliaryString = current.ToString();
                        auxiliaryToken = JsonParserToken.String;
                        goto case KnownJsonObjectType.HandleSetStringBufferOptimization;
                    }
                    case KnownJsonObjectType.String:
                    {
                        auxiliaryString = (string)current;
                        auxiliaryToken = JsonParserToken.String;
                        goto case KnownJsonObjectType.HandleSetStringBufferOptimization;
                    }
                    case KnownJsonObjectType.HandleSetStringBufferOptimization:
                    {
                        SetStringBuffer(auxiliaryString);
                        state.CurrentTokenType = auxiliaryToken;
                        return true;
                    }
                    case KnownJsonObjectType.HandleSetStringBufferNumberOptimization:
                    {
                        auxiliaryString = EnsureDecimalPlace(auxiliaryDouble, auxiliaryDouble.ToString("R", CultureInfo.InvariantCulture));
                        auxiliaryToken = JsonParserToken.Float;
                        goto case KnownJsonObjectType.HandleSetStringBufferOptimization;
                    }
                    case KnownJsonObjectType.Float:
                    {
                        auxiliaryDouble = (float)current;
                        goto case KnownJsonObjectType.HandleSetStringBufferNumberOptimization;
                    }
                    case KnownJsonObjectType.Double:
                    {
                        auxiliaryDouble = (double)current;
                        goto case KnownJsonObjectType.HandleSetStringBufferNumberOptimization;
                    }
                    case KnownJsonObjectType.Decimal:
                    {
                        var d = (decimal)current;

                        if (DecimalHelper.Instance.IsDouble(ref d) || d > long.MaxValue || d < long.MinValue)
                        {
                            auxiliaryString = EnsureDecimalPlace((double)d, d.ToString(CultureInfo.InvariantCulture));
                            auxiliaryToken = JsonParserToken.Float;
                            goto case KnownJsonObjectType.HandleSetStringBufferOptimization;
                        }

                        current = (long)d;
                        goto case KnownJsonObjectType.Int64;
                    }
#if NET6_0_OR_GREATER
                    case KnownJsonObjectType.Half:
                    {
                        auxiliaryDouble = (double)(Half)(current);
                        goto case KnownJsonObjectType.HandleSetStringBufferNumberOptimization;
                    }
#endif
                    case KnownJsonObjectType.DateTime:
                    {
                        auxiliaryString = ((DateTime)current).GetDefaultRavenFormat();
                        auxiliaryToken = JsonParserToken.String;
                        goto case KnownJsonObjectType.HandleSetStringBufferOptimization;
                    }
                    case KnownJsonObjectType.DateTimeOffset:
                    {
                        auxiliaryString = ((DateTimeOffset)current).ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                        auxiliaryToken = JsonParserToken.String;
                        goto case KnownJsonObjectType.HandleSetStringBufferOptimization;
                    }
                    case KnownJsonObjectType.TimeSpan:
                    {
                        auxiliaryString = ((TimeSpan)current).ToString("c");
                        auxiliaryToken = JsonParserToken.String;
                        goto case KnownJsonObjectType.HandleSetStringBufferOptimization;
                    }
#if FEATURE_DATEONLY_TIMEONLY_SUPPORT
                    case KnownJsonObjectType.TimeOnly:
                    {
                        auxiliaryString = ((TimeOnly)current).ToString(DefaultFormat.TimeOnlyFormatToWrite);
                        auxiliaryToken = JsonParserToken.String;
                        goto case KnownJsonObjectType.HandleSetStringBufferOptimization;
                    }

                    case KnownJsonObjectType.DateOnly:
                    {
                        auxiliaryString = ((DateOnly)current).ToString(DefaultFormat.DateOnlyFormatToWrite);
                        auxiliaryToken = JsonParserToken.String;
                        goto case KnownJsonObjectType.HandleSetStringBufferOptimization;
                    }
#endif

                    case KnownJsonObjectType.ListOfLong:
                    {
                        current = HandleLongsList((List<long>)current);
                        goto case KnownJsonObjectType.DynamicJsonArray;
                    }
                    case KnownJsonObjectType.ListOfDouble:
                    {
                        current = HandleDoublesList((List<double>)current);
                        goto case KnownJsonObjectType.DynamicJsonArray;
                    }
                    case KnownJsonObjectType.DictionaryStringLong:
                    {
                        current = HandleDictionaryType((Dictionary<string, long>)current);
                        goto case KnownJsonObjectType.DynamicJsonArray;
                    }

                    case KnownJsonObjectType.IEnumerableObject:
                    {
                        var enumerable = (IEnumerable<object>)current;
                        current = new DynamicJsonArray(enumerable);
                        goto case KnownJsonObjectType.DynamicJsonArray;
                    }

                    case KnownJsonObjectType.BlittableJsonReaderArray:
                    {
                        var bjra = (BlittableJsonReaderArray)current;
                        bjra.Modifications ??= new DynamicJsonArray();

                        var modifications = bjra.Modifications;

                        if (_seenValues.Add(bjra.Modifications))
                        {
                            _elements.Push(bjra);
                            modifications.SourceIndex = modifications.SkipOriginalArray ? bjra.Length : -1;
                            modifications.ModificationsIndex = 0;
                            state.CurrentTokenType = JsonParserToken.StartArray;
                            return true;
                        }

                        modifications.SourceIndex++;
                        if (modifications.SourceIndex < bjra.Length)
                        {
                            if (modifications.Removals != null && modifications.Removals.Contains(modifications.SourceIndex))
                                continue;

                            current = bjra[modifications.SourceIndex];
                            _elements.Push(bjra);
                            continue;
                        }
                        current = modifications;
                        goto case KnownJsonObjectType.DynamicJsonArray;
                    }

                    case KnownJsonObjectType.DynamicJsonArray:
                    {
                        var array = (DynamicJsonArray)current;
                        if (_seenValues.Add(array))
                        {
                            array.SourceIndex = -1;
                            array.ModificationsIndex = 0;
                            state.CurrentTokenType = JsonParserToken.StartArray;
                            _elements.Push(array);
                            return true;
                        }
                        if (array.ModificationsIndex >= array.Items.Count)
                        {
                            _seenValues.Remove(array);
                            state.CurrentTokenType = JsonParserToken.EndArray;
                            return true;
                        }
                        _elements.Push(array);
                        current = array.Items[array.ModificationsIndex++];
                        continue;
                    }

                    case KnownJsonObjectType.BlittableJsonReaderVector:
                    {
                        var bjrv = (BlittableJsonReaderVector)current;
                        bjrv.Modifications ??= new DynamicJsonArray();

                        var modifications = bjrv.Modifications;

                        if (_seenValues.Add(modifications))
                        {
                            _elements.Push(bjrv);
                            modifications.SourceIndex = modifications.SkipOriginalArray ? bjrv.Length : -1;
                            modifications.ModificationsIndex = 0;
                            state.CurrentTokenType = JsonParserToken.StartArray;
                            return true;
                        }

                        modifications.SourceIndex++;
                        if (modifications.SourceIndex < bjrv.Length)
                        {
                            if (modifications.Removals != null && modifications.Removals.Contains(modifications.SourceIndex))
                                continue;

                            current = bjrv[modifications.SourceIndex];
                            _elements.Push(bjrv);
                            continue;
                        }

                        current = modifications;
                        goto case KnownJsonObjectType.DynamicJsonArray;
                    }

                    case KnownJsonObjectType.IBlittableJsonContainer:
                    {
                        var dbj = (IBlittableJsonContainer)current;
                        current = dbj.BlittableJson;
                        if (current == null)
                            continue;

                        goto case KnownJsonObjectType.BlittableJsonReaderObject;
                    }

                    case KnownJsonObjectType.RawBlob:
                    {
                        var bs = (BlittableJsonReaderObject.RawBlob)current;
                        state.StringBuffer = bs.Address;
                        state.StringSize = bs.Length;
                        state.CompressedSize = null;// don't even try
                        state.CurrentTokenType = JsonParserToken.Blob;
                        return true;
                    }
                }
            }

            DynamicJsonArray HandleDictionaryType(Dictionary<string, long> dsl)
            {
                var dja = new DynamicJsonArray();
                foreach (var item in dsl)
                    dja.Add(new DynamicJsonValue(item.Key, item.Value));
                return dja;
            }

            DynamicJsonArray HandleDoublesList(List<double> dd)
            {
                var dja = new DynamicJsonArray();
                foreach (var item in dd)
                    dja.Add(item);

                return dja;
            }

            DynamicJsonArray HandleLongsList(List<long> dd)
            {
                var dja = new DynamicJsonArray();
                foreach (var item in dd)
                    dja.Add(item);

                return dja;
            }
        }

        private void ReadEscapePositions(byte* buffer, int escapeSequencePos)
        {
            var escapePositions = _state.EscapePositions;
            escapePositions.Clear();
            var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(buffer, ref escapeSequencePos);
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(buffer, ref escapeSequencePos);
                escapePositions.Add(bytesToSkip);
            }
        }

        private void ThrowOnDisposed()
        {
            throw new ObjectDisposedException(nameof(ObjectJsonParser));
        }

        private void SetStringBuffer(string str)
        {
            // max possible size - we avoid using GetByteCount because profiling showed it to take 2% of runtime
            // the buffer might be a bit longer, but we'll reuse it, and it is better than the computing cost
           
            int escapePositionsSize = StringUtils.FindMaxEscapePositionSize(str.AsSpan());

            int byteCount = str.Length * 5 + escapePositionsSize;
            if (_currentStateBuffer == null || _currentStateBuffer.SizeInBytes < byteCount)
            {
                byteCount = Encodings.Utf8.GetMaxByteCount(str.Length);

                // If we do not have a buffer or the buffer is too small, return the memory and get more.
                var size = byteCount + escapePositionsSize;
                if (_currentStateBuffer == null || _currentStateBuffer.SizeInBytes < size)
                {
                    if (_currentStateBuffer != null)
                        _ctx.ReturnMemory(_currentStateBuffer);
                    _currentStateBuffer = _ctx.GetMemory(size);
                    Debug.Assert(_currentStateBuffer != null && _currentStateBuffer.Address != null);
                }
            }

            _state.StringBuffer = _currentStateBuffer.Address;

            fixed (char* pChars = str)
            {
                _state.StringSize = Encodings.Utf8.GetBytes(pChars, str.Length, _state.StringBuffer, _currentStateBuffer.SizeInBytes);
                _state.CompressedSize = null; // don't even try
                _state.FindEscapedPositions(_state.StringBuffer, _state.StringSize, escapePositionsSize);

                var escapePos = _state.StringBuffer + _state.StringSize;
                _state.WriteEscapePositionsTo(escapePos);
            }
        }

        private static string EnsureDecimalPlace(double value, string text)
        {
            if (double.IsNaN(value) || double.IsInfinity(value) || text.IndexOf('.') != -1 || text.IndexOf('E') != -1 || text.IndexOf('e') != -1)
                return text;

            return text + ".0";
        }

        public void ValidateFloat()
        {
            // all floats are valid by definition
        }

        public string GenerateErrorState()
        {
            var last = _elements.LastOrDefault();
            return last?.ToString() ?? string.Empty;
        }

        public  OnStringReadDelegate OnStringRead
        {
            set { throw new NotSupportedException(); }
        }
    }
}
