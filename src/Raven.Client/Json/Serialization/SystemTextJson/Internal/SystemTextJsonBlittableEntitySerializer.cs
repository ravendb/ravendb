using System;
using System.Text.Json;
using Raven.Client.Documents.Identity;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SystemTextJsonBlittableEntitySerializer
    {
        private readonly LightWeightThreadLocal<JsonSerializerOptions> _options;

        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        public SystemTextJsonBlittableEntitySerializer(SystemTextJsonSerializationConventions conventions)
        {
            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions.Conventions, generateIdAsync: null);
            _options = new LightWeightThreadLocal<JsonSerializerOptions>(() => conventions.CreateJsonSerializerOptions());
        }

        public object EntityFromJsonStream(Type type, BlittableJsonReaderObject json)
        {
            using var reader = new SystemTextJsonBlittableReader();
            reader.Initialize(json);

            object entity;
            try
            {
                entity = JsonSerializer.Deserialize(reader.GetUtf8Json(), type, _options.Value);
            }
            finally
            {
                // Return native memory immediately - we're done with the UTF-8 bytes
                reader.ReturnMemory();
            }

            if (entity != null)
                TrySetIdentityFromMetadata(entity, json);

            return entity;
        }

        private void TrySetIdentityFromMetadata(object entity, BlittableJsonReaderObject json)
        {
            if (json.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false || metadata == null)
                return;

            if (metadata.TryGet(Constants.Documents.Metadata.Id, out string id) && id != null)
            {
                bool isProjection = metadata.TryGet(Constants.Documents.Metadata.Projection, out bool projection) && projection;

                _generateEntityIdOnTheClient.TrySetIdentity(entity, id, isProjection);
                return;
            }

            if (json.TryGet(Constants.Documents.Metadata.Id, out string topLevelId) && topLevelId != null)
            {
                if (_generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out string existing) && existing != null)
                    return;

                _generateEntityIdOnTheClient.TrySetIdentity(entity, topLevelId);
            }
        }
    }
}
