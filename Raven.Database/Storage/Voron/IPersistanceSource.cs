namespace Raven.Database.Storage.Voron
{
	using System;

	using global::Voron.Impl;

	public interface IPersistanceSource : IDisposable
	{
		IVirtualPager Pager { get; }

		bool CreatedNew { get; }
	}
}