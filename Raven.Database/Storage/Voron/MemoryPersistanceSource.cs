namespace Raven.Database.Storage.Voron
{
	using global::Voron.Impl;

	public class MemoryPersistanceSource : IPersistanceSource
	{
		public MemoryPersistanceSource()
		{
			CreatedNew = true;
			Pager = new PureMemoryPager();
		}

		public IVirtualPager Pager { get; private set; }

		public bool CreatedNew { get; private set; }

		public void Dispose()
		{
			if (Pager != null)
				Pager.Dispose();
		}
	}
}