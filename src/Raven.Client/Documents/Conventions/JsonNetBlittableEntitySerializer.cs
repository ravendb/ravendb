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

        public JsonNetBlittableEntitySerializer(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        public object EntityFromJsonStream(Type type, BlittableJsonReaderObject jsonObject)
        {
            if (_reader == null)
                _reader = new BlittableJsonReader();
            if (_serializer == null)
                _serializer = _conventions.CreateSerializer();

            _reader.Init(jsonObject);

            return _serializer.Deserialize(_reader, type);
        }
    }
}