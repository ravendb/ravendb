using Voron;

namespace Raven.Database.Server.RavenFS.Storage.Voron.Impl
{
    public interface IPersistenceSource
	{
		StorageEnvironmentOptions Options { get; }

		bool CreatedNew { get; }
	}
}