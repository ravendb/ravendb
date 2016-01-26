using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Server.Json.Parsing
{

    public class DynamicJsonBuilder
    {
        private readonly List<Tuple<string, object>> _properties = new List<Tuple<string, object>>();

        public void PushContents(Stack<object> stack)
        {
            foreach (var prop in _properties)
            {
                stack.Push(prop);
            }
        }

        public string this[string name]
        {
            set
            {
                _properties.Add(Tuple.Create<string, object>(name, value));
            }
        }
    }
    public class ObjectJsonParser : IJsonParser
    {
        private readonly JsonParserState _state;
        private readonly Stack<object> _elements = new Stack<object>();
        private static readonly object _endArray = new object();
        private static readonly object _endObject = new object();

        public ObjectJsonParser(JsonParserState state, DynamicJsonBuilder jsonBuilder)
        {
            _state = state;
            _elements.Push(jsonBuilder);
        }

        public void Dispose()
        {

        }

        public void Read()
        {
            if (_elements.Count == 0)
                throw new EndOfStreamException();

            var current = _elements.Pop();

            var builder = current as DynamicJsonBuilder;
            if (builder != null)
            {
                _elements.Push(_endObject);
                builder.PushContents(_elements);
                _state.Current = JsonParserToken.StartObject;
                return;
            }
            if (current == _endObject)
            {
                _state.Current = JsonParserToken.EndObject;
                return;
            }
            if (current == _endArray)
            {
                _state.Current = JsonParserToken.EndArray;
                return;
            }
            var tuple = current as Tuple<string,object>;
            if (tuple != null)
            {
                _elements.Push(tuple.Item2);

                return;
            }

        public void ValidateFloat()
        {
            // all floats are valid by definition
        }
    }
}