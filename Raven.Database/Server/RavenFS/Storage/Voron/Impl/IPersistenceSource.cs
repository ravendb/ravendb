using System;

using Voron;

namespace Raven.Database.Server.RavenFS.Storage.Voron.Impl
{
    public interface IPersistenceSource : IDisposable
	{
		StorageEnvironmentOptions Options { get; }

		bool CreatedNew { get; }
	}
}