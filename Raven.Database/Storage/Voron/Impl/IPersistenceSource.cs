namespace Raven.Database.Storage.Voron.Impl
{
	using global::Voron;

	public interface IPersistenceSource
	{
		StorageEnvironmentOptions Options { get; }

		bool CreatedNew { get; }
	}
}