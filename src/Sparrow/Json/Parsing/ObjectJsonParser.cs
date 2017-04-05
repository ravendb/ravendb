using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Sparrow.Collections;
using Sparrow.Extensions;
using Sparrow.Utils;

namespace Sparrow.Json.Parsing
{
    public class DynamicJsonValue
    {
        public const string TypeFieldName = "$type";

        public int SourceIndex = -1;
        public int[] SourceProperties;

        public readonly Queue<Tuple<string, object>> Properties = new Queue<Tuple<string, object>>();
        public HashSet<int> Removals;
        public bool AlreadySeen;
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
                if (_source.Modifications != null)
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
                if (_source != null)
                    Remove(name);
                Properties.Enqueue(Tuple.Create(name, value));
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
    }

    public class DynamicJsonArray : IEnumerable<object>
    {
        public int SourceIndex = -1;
        public readonly Queue<object> Items;
        public List<int> Removals;
        public bool AlreadySeen;

        public DynamicJsonArray()
        {
            Items = new Queue<object>();
        }

        public DynamicJsonArray(IEnumerable<object> collection)
        {
            Items = new Queue<object>(collection);
        }

        public void RemoveAt(int index)
        {
            if (Removals == null)
                Removals = new List<int>();
            Removals.Add(index);
        }

        public void Add(object obj)
        {
            Items.Enqueue(obj);
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
    }

    public unsafe class ObjectJsonParser : IJsonParser
    {
        private static readonly UTF8Encoding Utf8Encoding = new UTF8Encoding();

        private readonly JsonParserState _state;
        private readonly JsonOperationContext _ctx;
        private readonly FastStack<object> _elements = new FastStack<object>();

        private bool _disposed;
        private AllocatedMemoryData _currentStateBuffer;

        public void Reset(object root)
        {
            if (_currentStateBuffer != null)
            {
                _ctx.ReturnMemory(_currentStateBuffer);
                _currentStateBuffer = null;
            }

            _elements.Clear();   
            
            if(root != null)         
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
            if(_currentStateBuffer != null)
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
                var value = current as DynamicJsonValue;
                if (value != null)
                {
                    if (value.AlreadySeen == false)
                    {
#if DEBUG
                        if(value._source != null)
                            throw new InvalidOperationException("Trying to directly modify a DynamicJsonValue with a source, but you need to place the source (blittable), not the json value in the parent.");
#endif
                        value.AlreadySeen = true;
                        _state.CurrentTokenType = JsonParserToken.StartObject;
                        _elements.Push(value);
                        return true;
                    }
                    if (value.Properties.Count == 0)
                    {
                        _state.CurrentTokenType = JsonParserToken.EndObject;
                        return true;
                    }
                    _elements.Push(value);
                    current = value.Properties.Dequeue();
                    continue;
                }

                var array = current as DynamicJsonArray;
                if (array != null)
                {
                    if (array.AlreadySeen == false)
                    {
                        array.AlreadySeen = true;
                        _state.CurrentTokenType = JsonParserToken.StartArray;
                        _elements.Push(array);
                        return true;
                    }
                    if (array.Items.Count == 0)
                    {
                        _state.CurrentTokenType = JsonParserToken.EndArray;
                        return true;
                    }
                    _elements.Push(array);
                    current = array.Items.Dequeue();
                    continue;
                }

                var tuple = current as Tuple<string, object>;
                if (tuple != null)
                {
                    _elements.Push(tuple.Item2);
                    current = tuple.Item1;
                    continue;
                }

                var prop = current as Tuple<LazyStringValue, object>;
                if (prop != null)
                {
                    _elements.Push(prop.Item2);
                    current = prop.Item1;
                    continue;
                }

                var bjro = current as BlittableJsonReaderObject;
                if (bjro != null)
                {
                    if (bjro.Modifications == null)
                        bjro.Modifications = new DynamicJsonValue();
                    if (bjro.Modifications.AlreadySeen == false)
                    {
                        _elements.Push(bjro);
                        bjro.Modifications.AlreadySeen = true;
                        bjro.Modifications.SourceProperties = bjro.GetPropertiesByInsertionOrder();
                        _state.CurrentTokenType = JsonParserToken.StartObject;
                        return true;
                    }

                    var modifications = bjro.Modifications;
                    modifications.SourceIndex++;
                    var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                    if (modifications.SourceIndex < modifications.SourceProperties.Length)
                    {
                        var propIndex = modifications.SourceProperties[modifications.SourceIndex];
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
                    current = modifications;
                    continue;
                }

                var bjra = current as BlittableJsonReaderArray;
                if (bjra != null)
                {
                    if (bjra.Modifications == null)
                        bjra.Modifications = new DynamicJsonArray();
                    if (bjra.Modifications.AlreadySeen == false)
                    {
                        _elements.Push(bjra);
                        bjra.Modifications.AlreadySeen = true;
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

                var dbj = current as IBlittableJsonContainer;
                if (dbj != null)
                {
                    current = dbj.BlittableJson;
                    continue;
                }

                var enumerable = current as IEnumerable<object>;
                if (enumerable != null)
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

                if (current is LazyCompressedStringValue lcsv)
                {
                    _state.StringBuffer = lcsv.Buffer;
                    _state.StringSize = lcsv.UncompressedSize;
                    _state.CompressedSize = lcsv.CompressedSize;
                    _state.CurrentTokenType = JsonParserToken.String;
                    ReadEscapePositions(lcsv.Buffer, lcsv.CompressedSize);
                    return true;
                }

                if (current is LazyDoubleValue ldv)
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

                if (current is int || current is byte || current is short)
                {
                    _state.Long = Convert.ToInt32(current);
                    _state.CurrentTokenType = JsonParserToken.Integer;
                    return true;
                }

                if (current is long)
                {
                    _state.Long = (long)current;
                    _state.CurrentTokenType = JsonParserToken.Integer;
                    return true;
                }

                if (current is bool)
                {
                    _state.CurrentTokenType = ((bool)current) ? JsonParserToken.True : JsonParserToken.False;
                    return true;
                }

                if (current is float)
                {
                    var d = (double)(float)current;
                    var s = EnsureDecimalPlace(d, d.ToString("R", CultureInfo.InvariantCulture));
                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.Float;
                    return true;
                }

                if (current is double)
                {
                    var d = (double)current;
                    var s = EnsureDecimalPlace(d, d.ToString("R", CultureInfo.InvariantCulture));
                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.Float;
                    return true;
                }

                if (current is DateTime)
                {
                    var dateTime = (DateTime)current;
                    var s = dateTime.GetDefaultRavenFormat(isUtc: dateTime.Kind == DateTimeKind.Utc);

                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.String;
                    return true;
                }

                if (current is DateTimeOffset)
                {
                    var dateTime = (DateTimeOffset)current;
                    var s = dateTime.ToString("o");

                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.String;
                    return true;
                }

                if (current is TimeSpan)
                {
                    var timeSpan = (TimeSpan)current;
                    var s = timeSpan.ToString();

                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.String;
                    return true;
                }

                if (current is decimal)
                {
                    var d = (decimal)current;

                    if (DecimalHelper.Instance.IsDouble(ref d))
                    {
                        var s = EnsureDecimalPlace((double)d, d.ToString(CultureInfo.InvariantCulture));
                        SetStringBuffer(s);
                        _state.CurrentTokenType = JsonParserToken.Float;
                        return true;
                    }

                    current = (long)d;
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
            int byteCount = Utf8Encoding.GetMaxByteCount(str.Length);
            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(str);

            // If we do not have a buffer or the buffer is too small, return the memory and get more.
            var size = byteCount + escapePositionsSize;
            if (_currentStateBuffer == null || _currentStateBuffer.SizeInBytes < size)
            {
                if (_currentStateBuffer != null)
                    _ctx.ReturnMemory(_currentStateBuffer);
                _currentStateBuffer = _ctx.GetMemory(size);
            }

            _state.StringBuffer = _currentStateBuffer.Address;

            fixed (char* pChars = str)
            {
                _state.StringSize = Utf8Encoding.GetBytes(pChars, str.Length, _state.StringBuffer, byteCount);
                _state.CompressedSize = null; // don't even try
                _state.FindEscapePositionsIn(_state.StringBuffer, _state.StringSize, escapePositionsSize);

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
            return string.Empty;
        }
    }
}