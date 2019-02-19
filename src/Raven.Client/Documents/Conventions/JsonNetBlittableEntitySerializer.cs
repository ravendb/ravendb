using System;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Identity;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Conventions
{
    internal class JsonNetBlittableEntitySerializer
    {
        private readonly ThreadLocal<BlittableJsonReader> _reader;

        private readonly ThreadLocal<JsonSerializer> _serializer;

        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        public JsonNetBlittableEntitySerializer(DocumentConventions conventions)
        {
            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions, null);
            _serializer = new ThreadLocal<JsonSerializer>(conventions.CreateSerializer);
            _reader = new ThreadLocal<BlittableJsonReader>(() => new BlittableJsonReader());
        }

        public object EntityFromJsonStream(Type type, BlittableJsonReaderObject jsonObject)
        {
            _reader.Value.Init(jsonObject);
            _serializer.Value.NullValueHandling = NullValueHandling.Ignore;

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
                return _serializer.Value.Deserialize(_reader.Value, type);
            }
        }
    }
}
