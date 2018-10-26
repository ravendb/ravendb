using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Identity;
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
        [ThreadStatic]
        private static Action<JsonSerializer> _customize;

        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        public JsonNetBlittableEntitySerializer(DocumentConventions conventions)
        {
            _conventions = conventions;
            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions, null);
        }

        public static void CleanThreadStatics()
        {
            _reader = null;
            _serializer = null;
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
            using (DefaultRavenContractResolver.RegisterExtensionDataSetter((o, key, value) =>
            {
                JToken id;
                if (key == Constants.Documents.Metadata.Key && value is JObject json)
                {
                    if (json.TryGetValue(Constants.Documents.Metadata.Id, out  id))
                    {
                        if (_generateEntityIdOnTheClient.TryGetIdFromInstance(o, out var existing) &&
                            existing != null)
                            return;
                        _generateEntityIdOnTheClient.TrySetIdentity(o, id.Value<string>());
                    }
                }

                if (key == Constants.Documents.Metadata.Id)
                {
                    id = value as JToken;
                    if (id == null)
                        return;

                    if (_generateEntityIdOnTheClient.TryGetIdFromInstance(o, out var existing) &&
                        existing != null)
                        return;
                    _generateEntityIdOnTheClient.TrySetIdentity(o, id.Value<string>());
                }
            }))
            {
                return _serializer.Deserialize(_reader, type);
            }

        }
    }
}
