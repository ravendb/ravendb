using System;
using System.Text.Json;
using Raven.Client.Documents.Identity;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SystemTextJsonBlittableEntitySerializer
    {
        private readonly SystemTextJsonSerializationConventions _conventions;

        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        public SystemTextJsonBlittableEntitySerializer(SystemTextJsonSerializationConventions conventions)
        {
            _conventions = conventions;
            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions.Conventions, generateIdAsync: null);
        }

        public object EntityFromJsonStream(Type type, BlittableJsonReaderObject json)
        {
            object entity;
            using (var reader = new SystemTextJsonBlittableReader())
            {
                reader.Initialize(json);
                entity = JsonSerializer.Deserialize(reader.GetUtf8Json(), type, _conventions.Options);
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
