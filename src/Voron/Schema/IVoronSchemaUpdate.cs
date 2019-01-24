using Voron.Impl.FileHeaders;

namespace Voron.Schema
{
    public interface IVoronSchemaUpdate
    {
        bool Update(int currentVersion, StorageEnvironmentOptions options, HeaderAccessor headerAccessor, out int versionAfterUpgrade);
    }
}
