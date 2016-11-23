//using System.Collections.Generic;
//using Sparrow.Json.Parsing;

//namespace Raven.Client.Json
//{
//    public class BlittableJsonWriterParser:IJsonParser
//    {
//        private readonly Stack<JsonParserToken?> _elements = new Stack<JsonParserToken?>();

//        private void InitializeParser()
//        {
//            _elements.Push(null);
//        }

//        public bool Read()
//        {
//            return true;
//        }

//        public void ValidateFloat()
//        {
            
//        }

//        public void SetStartObject()
//        {
//            _elements.Push(JsonParserToken.StartObject);
//            _jsonParserState.CurrentTokenType = JsonParserToken.StartObject;
//        }

//        public void SetStartArray()
//        {
//            _elements.Push(JsonParserToken.StartArray);
//            _jsonParserState.CurrentTokenType = JsonParserToken.StartArray;
//        }

//        public void SetEnd()
//        {
//            var last = _elements.Peek();

//            if (last.Value == JsonParserToken.StartArray)
//            {
//                _elements.Pop();
//                _jsonParserState.CurrentTokenType = JsonParserToken.EndArray;
//            }
//            else if (last.Value == JsonParserToken.StartObject)
//            {
//                _elements.Pop();
//                _jsonParserState.CurrentTokenType = JsonParserToken.EndObject;
//            }
//        }

//        public void SetEndArray()
//        {
//            var last = _elements.Peek();

//            if (last.Value == JsonParserToken.StartArray)
//            {
//                _elements.Pop();
//                _jsonParserState.CurrentTokenType = JsonParserToken.EndArray;
//            }
//        }

//        public void Dispose()
//        {
            
//        }
//    }
//}