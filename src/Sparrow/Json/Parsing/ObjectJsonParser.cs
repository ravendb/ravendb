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

    public class DynamicJsonValue
    {
        public const string TypeFieldName = "$type";

        public int SourceIndex = -1;
        public BlittableJsonReaderObject.InsertionOrderProperties SourceProperties;

        public int ModificationsIndex = 0;
        public readonly List<(string Name, object Value)> Properties = new List<(string Name, object Value)>();
        public HashSet<int> Removals;
        internal readonly BlittableJsonReaderObject _source;

        public DynamicJsonValue()
        {
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

            if (Removals == null)
                Removals = new HashSet<int>();
            Removals.Add(propertyIndex);
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
    }

    public class DynamicJsonArray : IEnumerable<object>, IDisposable
    {
        public int SourceIndex = -1;
        public int ModificationsIndex;
        public readonly List<object> Items;
        public List<int> Removals;

        public DynamicJsonArray()
        {
            Items = new List<object>();
        }

        public DynamicJsonArray(IEnumerable<object> collection)
        {
            Items = new List<object>(collection);
        }

        public void RemoveAt(int index)
        {
            if (Removals == null)
                Removals = new List<int>();
            Removals.Add(index);
        }

        public void Add(object obj)
        {
#if DEBUG
            if (obj != null &&
                obj.GetType().FullName == "Raven.Server.Documents.Document")
            {
                throw new InvalidOperationException("Cannot add Document to DynamicJsonArray");
            }
#endif
            Items.Add(obj);
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

    public unsafe class ObjectJsonParser : IJsonParser
    {
        private readonly JsonParserState _state;
        private readonly JsonOperationContext _ctx;
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

            _elements.Clear();
            _seenValues.Clear();

            if (root != null)
                _elements.Push(root);
        }

        public ObjectJsonParser(JsonParserState state, JsonOperationContext ctx)
        {
            _state = state;
            _ctx = ctx;
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            if (_currentStateBuffer != null)
                _ctx.ReturnMemory(_currentStateBuffer);
        }

        public bool Read()
        {
            if (_disposed)
                ThrowOnDisposed();

            if (_elements.Count == 0)
                throw new EndOfStreamException();

            var current = _elements.Pop();

            while (true)
            {
                if (current is IDynamicJson idj)
                {
                    current = idj.ToJson();
                }

                if (current is DynamicJsonValue value)
                {
                    if (_seenValues.Add(value))
                    {
#if DEBUG
                        if (value._source != null)
                            throw new InvalidOperationException("Trying to directly modify a DynamicJsonValue with a source, but you need to place the source (blittable), not the json value in the parent.");
#endif
                        value.SourceIndex = -1;
                        _state.CurrentTokenType = JsonParserToken.StartObject;
                        value.ModificationsIndex = 0;
                        _elements.Push(value);
                        return true;
                    }
                    if (value.ModificationsIndex >= value.Properties.Count)
                    {
                        _seenValues.Remove(value);
                        _state.CurrentTokenType = JsonParserToken.EndObject;
                        return true;
                    }
                    _elements.Push(value);
                    current = value.Properties[value.ModificationsIndex++];
                    continue;
                }

                if (current is DynamicJsonArray array)
                {
                    if (_seenValues.Add(array))
                    {
                        array.SourceIndex = -1;
                        array.ModificationsIndex = 0;
                        _state.CurrentTokenType = JsonParserToken.StartArray;
                        _elements.Push(array);
                        return true;
                    }
                    if (array.ModificationsIndex >= array.Items.Count)
                    {
                        _seenValues.Remove(array);
                        _state.CurrentTokenType = JsonParserToken.EndArray;
                        return true;
                    }
                    _elements.Push(array);
                    current = array.Items[array.ModificationsIndex++];
                    continue;
                }

                if (current is ValueTuple<string, object> vt)
                {
                    _elements.Push(vt.Item2);
                    current = vt.Item1;
                    continue;
                }

                if (current is BlittableJsonReaderObject bjro)
                {
                    if (bjro.Modifications == null)
                        bjro.Modifications = new DynamicJsonValue(bjro);
                    if (_seenValues.Add(bjro.Modifications))
                    {
                        _elements.Push(bjro);
                        bjro.Modifications.SourceIndex = -1;
                        bjro.Modifications.ModificationsIndex = 0;
                        bjro.Modifications.SourceProperties = bjro.GetPropertiesByInsertionOrder();
                        _state.CurrentTokenType = JsonParserToken.StartObject;
                        return true;
                    }

                    var modifications = bjro.Modifications;
                    modifications.SourceIndex++;
                    var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                    if (modifications.SourceIndex < modifications.SourceProperties.Size)
                    {
                        var propIndex = modifications.SourceProperties.Properties[modifications.SourceIndex];
                        if (modifications.Removals != null && modifications.Removals.Contains(propIndex))
                        {
                            continue;
                        }
                        bjro.GetPropertyByIndex(propIndex, ref propDetails);
                        _elements.Push(bjro);
                        _elements.Push(propDetails.Value);
                        current = propDetails.Name;
                        continue;
                    }
                    modifications.SourceProperties.Dispose();
                   current = modifications;
                    continue;
                }

                if (current is BlittableJsonReaderArray bjra)
                {
                    if (bjra.Modifications == null)
                        bjra.Modifications = new DynamicJsonArray();

                    if (_seenValues.Add(bjra.Modifications))
                    {
                        _elements.Push(bjra);
                        bjra.Modifications.SourceIndex = -1;
                        bjra.Modifications.ModificationsIndex = 0;
                        _state.CurrentTokenType = JsonParserToken.StartArray;
                        return true;
                    }

                    var modifications = bjra.Modifications;
                    modifications.SourceIndex++;
                    if (modifications.SourceIndex < bjra.Length)
                    {
                        if (modifications.Removals != null && modifications.Removals.Contains(modifications.SourceIndex))
                        {
                            continue;
                        }
                        _elements.Push(bjra);
                        current = bjra[modifications.SourceIndex];
                        continue;
                    }
                    current = modifications;
                    continue;

                }

                if (current is IBlittableJsonContainer dbj)
                {
                    current = dbj.BlittableJson;
                    continue;
                }

                if (current is IEnumerable<object> enumerable)
                {
                    current = new DynamicJsonArray(enumerable);
                    continue;
                }

                if (current is LazyStringValue lsv)
                {
                    _state.StringBuffer = lsv.Buffer;
                    _state.StringSize = lsv.Size;
                    _state.CompressedSize = null;// don't even try
                    _state.CurrentTokenType = JsonParserToken.String;
                    ReadEscapePositions(lsv.Buffer, lsv.Size);
                    return true;
                }

                if (current is BlittableJsonReaderObject.RawBlob bs)
                {
                    _state.StringBuffer = bs.Address;
                    _state.StringSize = bs.Length;
                    _state.CompressedSize = null;// don't even try
                    _state.CurrentTokenType = JsonParserToken.Blob;
                    return true;
                }

                if (current is LazyCompressedStringValue lcsv)
                {
                    _state.StringBuffer = lcsv.Buffer;
                    _state.StringSize = lcsv.UncompressedSize;
                    _state.CompressedSize = lcsv.CompressedSize;
                    _state.CurrentTokenType = JsonParserToken.String;
                    ReadEscapePositions(lcsv.Buffer, lcsv.CompressedSize);
                    return true;
                }

                if (current is LazyNumberValue ldv)
                {
                    _state.StringBuffer = ldv.Inner.Buffer;
                    _state.StringSize = ldv.Inner.Size;
                    _state.CompressedSize = null;// don't even try
                    _state.CurrentTokenType = JsonParserToken.Float;
                    ReadEscapePositions(ldv.Inner.Buffer, ldv.Inner.Size);
                    return true;
                }

                if (current is string str)
                {
                    SetStringBuffer(str);
                    _state.CurrentTokenType = JsonParserToken.String;
                    return true;
                }

                if (current is char @char)
                {
                    current = new string(@char, 1);
                    continue;
                }

                if (current is int || current is byte || current is sbyte || current is short || current is ushort)
                {
                    _state.Long = Convert.ToInt32(current);
                    _state.CurrentTokenType = JsonParserToken.Integer;
                    return true;
                }

                if (current is long l)
                {
                    _state.Long = l;
                    _state.CurrentTokenType = JsonParserToken.Integer;
                    return true;
                }

                if (current is ulong ul)
                {
                    _state.Long = (long)ul;
                    _state.CurrentTokenType = JsonParserToken.Integer;
                    return true;
                }

                if (current is uint ui)
                {
                    _state.Long = (long)ui;
                    _state.CurrentTokenType = JsonParserToken.Integer;
                    return true;
                }

                if (current is bool b)
                {
                    _state.CurrentTokenType = b ? JsonParserToken.True : JsonParserToken.False;
                    return true;
                }

                if (current is float f)
                {
                    var d = (double)f;
                    var s = EnsureDecimalPlace(d, d.ToString("R", CultureInfo.InvariantCulture));
                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.Float;
                    return true;
                }

                if (current is double d1)
                {
                    var s = EnsureDecimalPlace(d1, d1.ToString("R", CultureInfo.InvariantCulture));
                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.Float;
                    return true;
                }

                if (current is DateTime dateTime1)
                {
                    var s = dateTime1.GetDefaultRavenFormat();

                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.String;
                    return true;
                }

                if (current is DateTimeOffset dateTime)
                {
                    var s = dateTime.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.String;
                    return true;
                }

                if (current is TimeSpan timeSpan)
                {
                    var s = timeSpan.ToString("c");

                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.String;
                    return true;
                }

                if (current is decimal d2)
                {
                    var d = (decimal)current;

                    if (DecimalHelper.Instance.IsDouble(ref d) || d > long.MaxValue || d < long.MinValue)
                    {
                        var s = EnsureDecimalPlace((double)d2, d2.ToString(CultureInfo.InvariantCulture));
                        SetStringBuffer(s);
                        _state.CurrentTokenType = JsonParserToken.Float;
                        return true;
                    }

                    current = (long)d2;
                    continue;
                }

                if (current is List<long> ll)
                {
                    var dja = new DynamicJsonArray();
                    foreach (var item in ll)
                    {
                        dja.Add(item);
                    }
                    current = dja;
                    continue;
                }

                if (current is Dictionary<string, long> dsl)
                {
                    var dja = new DynamicJsonArray();
                    foreach (var item in dsl)
                    {
                        var djv = new DynamicJsonValue
                        {
                            [item.Key] = item.Value
                        };
                        dja.Add(djv);
                    }
                    current = dja;
                    continue;
                }

                if (current == null)
                {
                    _state.CurrentTokenType = JsonParserToken.Null;
                    return true;
                }

                if (current is Enum)
                {
                    current = current.ToString();
                    continue;
                }

                if (current is IDynamicJsonValueConvertible convertible)
                {
                    current = convertible.ToJson();
                    continue;
                }

                throw new InvalidOperationException("Got unknown type: " + current.GetType() + " " + current);
            }
        }

        private void ReadEscapePositions(byte* buffer, int escapeSequencePos)
        {
            _state.EscapePositions.Clear();
            var numberOfEscapeSequences = BlittableJsonReaderBase.ReadVariableSizeInt(buffer, ref escapeSequencePos);
            while (numberOfEscapeSequences > 0)
            {
                numberOfEscapeSequences--;
                var bytesToSkip = BlittableJsonReaderBase.ReadVariableSizeInt(buffer, ref escapeSequencePos);
                _state.EscapePositions.Add(bytesToSkip);
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
           
            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(str, out _);

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
                _state.FindEscapePositionsIn(_state.StringBuffer, ref _state.StringSize, escapePositionsSize);

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
    }
}
