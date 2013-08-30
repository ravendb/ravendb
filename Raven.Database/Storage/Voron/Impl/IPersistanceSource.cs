namespace Raven.Database.Storage.Voron.Impl
{
	using System;

	using global::Voron.Impl;

	public interface IPersistanceSource : IDisposable
	{
		IVirtualPager Pager { get; }

		bool CreatedNew { get; }
	}
}