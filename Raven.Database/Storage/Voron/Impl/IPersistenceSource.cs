namespace Raven.Database.Storage.Voron.Impl
{
	using System;

	using global::Voron;

	public interface IPersistenceSource : IDisposable
	{
		StorageEnvironmentOptions Options { get; }

		bool CreatedNew { get; }
	}
}