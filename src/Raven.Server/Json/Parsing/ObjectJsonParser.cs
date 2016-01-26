using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;

namespace Raven.Server.Json.Parsing
{
    public class DynamicJsonValue
    {
        public BlittableJsonReaderObject Source;
        public int SourceIndex = -1;
        public readonly Queue<Tuple<string, object>> Properties = new Queue<Tuple<string, object>>();
        public HashSet<int> Removals;
        public bool AlreadySeen;
    }

    public class DynamicJsonBuilder
    {
        public DynamicJsonValue Value = new DynamicJsonValue();

        public object this[string name]
        {
            set
            {
                Value.Properties.Enqueue(Tuple.Create(name, value));
            }
        }
    }

    public class DynamicJsonArray : IEnumerable<object>
    {
        public BlittableJsonReaderArray Source;
        public int SourceIndex = -1;
        public Queue<object> Items = new Queue<object>();
        public List<int> Removals;
        public bool AlreadySeen;

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

        public IEnumerator<object> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
    public unsafe class ObjectJsonParser : IJsonParser
    {
        private readonly JsonParserState _state;
        private readonly RavenOperationContext _ctx;
        private readonly Stack<object> _elements = new Stack<object>();

        public ObjectJsonParser(JsonParserState state, DynamicJsonBuilder jsonBuilder, RavenOperationContext ctx)
        {
            _state = state;
            _ctx = ctx;
            _elements.Push(jsonBuilder.Value);
        }

        public void Dispose()
        {

        }

        public void Read()
        {
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
                        value.AlreadySeen = true;
                        _state.CurrentTokenType = JsonParserToken.StartObject;
                        _elements.Push(value);
                        return;
                    }
                    if (value.Source != null && value.SourceIndex < value.Source.Count)
                    {
                        _elements.Push(value);
                        value.SourceIndex++;
                        current = value.Source.GetPropertyByIndex(value.SourceIndex);
                        continue;
                    }
                    if (value.Properties.Count == 0)
                    {
                        _state.CurrentTokenType = JsonParserToken.EndObject;
                        return;
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
                        return;
                    }
                    if (array.Items.Count == 0)
                    {
                        _state.CurrentTokenType = JsonParserToken.EndArray;
                        return;
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
                var lsv = current as LazyStringValue;
                if (lsv != null)
                {
                    _state.StringBuffer = lsv.Buffer;
                    _state.StringSize = lsv.Size;
                    _state.CurrentTokenType=JsonParserToken.String;
                    return;
                }
                var str = current as string;
                if (str != null)
                {
                    SetStringBuffer(str);
                    _state.CurrentTokenType = JsonParserToken.String;
                    return;
                }
                if (current is int)
                {
                    _state.Long = (int)current;
                    _state.CurrentTokenType = JsonParserToken.Integer;
                    return;
                }
                if (current is long)
                {
                    _state.Long = (long)current;
                    _state.CurrentTokenType = JsonParserToken.Integer;
                    return;
                }
                if (current is bool)
                {
                    _state.CurrentTokenType = ((bool)current) ? JsonParserToken.True : JsonParserToken.False;
                    return;
                }
                if (current is float)
                {
                    var d = (double)(float)current;
                    var s = EnsureDecimalPlace(d, d.ToString("R", CultureInfo.InvariantCulture));
                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.Float;
                    continue;
                }
                if (current is double)
                {
                    var d = (double)current;
                    var s = EnsureDecimalPlace(d, d.ToString("R", CultureInfo.InvariantCulture));
                    SetStringBuffer(s);
                    _state.CurrentTokenType = JsonParserToken.Float;
                    return;
                }
                if (current == null)
                {
                    _state.CurrentTokenType = JsonParserToken.Null;
                    return;
                }
                var djb = current as DynamicJsonBuilder;
                if (djb != null)
                {
                    current = djb.Value;
                    continue;
                }
                throw new InvalidOperationException("Got unknown type: " + current.GetType() + " " + current);
            }
        }

        private void SetStringBuffer(string str)
        {
            _state.StringSize = Encoding.UTF8.GetByteCount(str);
            int size;
            _state.StringBuffer = _ctx.GetNativeTempBuffer(_state.StringSize, out size);
            fixed (char* pChars = str)
            {
                Encoding.UTF8.GetBytes(pChars, str.Length, _state.StringBuffer, _state.StringSize);
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
    }
}