using System;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Identity;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Client.Json.Serialization.NewtonsoftJson.Internal
{
    internal class NewtonsoftJsonBlittableEntitySerializer
    {
        private readonly LightWeightThreadLocal<BlittableJsonReader> _reader;
        private readonly LightWeightThreadLocal<IJsonSerializer> _deserializer;

        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        internal BlittableJsonReader GetReaderForCurrentThread()
        {
            return _reader.Value;
        }

        public NewtonsoftJsonBlittableEntitySerializer(ISerializationConventions conventions)
        {
            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions.Conventions, null);
            _deserializer = new LightWeightThreadLocal<IJsonSerializer>(() => conventions.CreateDeserializer());
            _reader = new LightWeightThreadLocal<BlittableJsonReader>(() => new BlittableJsonReader());
        }

        public object EntityFromJsonStream(Type type, BlittableJsonReaderObject json)
        {
            _reader.Value.Initialize(json);

            using (DefaultRavenContractResolver.RegisterExtensionDataSetter((o, key, value) =>
            {
                JToken id;
                if (key == Constants.Documents.Metadata.Key && value is JObject json)
                {
                    if (json.TryGetValue(Constants.Documents.Metadata.Id, out id))
                    {
                        if (_generateEntityIdOnTheClient.TryGetIdFromInstance(o, out var existing) &&
                            existing != null)
                            return;

                        var isProjection = json.TryGetValue(Constants.Documents.Metadata.Projection, out var projection)
                                         && projection.Type == JTokenType.Boolean
                                         && projection.Value<bool>();

                        _generateEntityIdOnTheClient.TrySetIdentity(o, id.Value<string>(), isProjection);
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
                return _deserializer.Value.Deserialize(_reader.Value, type);
            }
        }
    }
}
