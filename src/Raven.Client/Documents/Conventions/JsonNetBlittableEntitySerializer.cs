using System;
using Newtonsoft.Json;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Conventions
{
    internal class JsonNetBlittableEntitySerializer
    {
        private readonly DocumentConventions _conventions;

        [ThreadStatic]
        private static BlittableJsonReader _reader;
        [ThreadStatic]
        private static JsonSerializer _serializer;

        private Action<JsonSerializer> _customize;

        public JsonNetBlittableEntitySerializer(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        public object EntityFromJsonStream(Type type, BlittableJsonReaderObject jsonObject)
        {
            if (_reader == null)
                _reader = new BlittableJsonReader();
            if (_serializer == null ||
                _conventions.CustomizeJsonSerializer != _customize)
            {
                // we need to keep track and see if the event has been changed,
                // if so, we'll need a new instance of the serializer
                _customize = _conventions.CustomizeJsonSerializer;
                _serializer = _conventions.CreateSerializer();
            }

            _reader.Init(jsonObject);

            return _serializer.Deserialize(_reader, type);
        }
    }
}