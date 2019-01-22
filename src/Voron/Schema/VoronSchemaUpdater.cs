using System;
using Voron.Global;
using Voron.Impl.FileHeaders;

namespace Voron.Schema
{
    public class VoronSchemaUpdater
    {
        private readonly HeaderAccessor _headerAccessor;
        private readonly StorageEnvironmentOptions _options;

        public VoronSchemaUpdater(HeaderAccessor headerAccessor, StorageEnvironmentOptions options)
        {
            _headerAccessor = headerAccessor;
            _options = options;
        }

        public unsafe void Update()
        {
            while (_headerAccessor.Get(header => header->Version) < Constants.CurrentVersion)
            {
                var currentVersion = _headerAccessor.Get(header => header->Version);
                var name = $"Voron.Schema.Updates.From{currentVersion}";

                var schemaUpdateType = typeof(IVoronSchemaUpdate).Assembly.GetType(name);
                if (schemaUpdateType == null)
                    break;

                var schemaUpdate = (IVoronSchemaUpdate)Activator.CreateInstance(schemaUpdateType);

                if (schemaUpdate.Update(currentVersion, _options, _headerAccessor, out var versionAfterUpdate) == false)
                    break;

                _headerAccessor.Modify(header => header->Version = versionAfterUpdate);
            }
        }
    }
}
