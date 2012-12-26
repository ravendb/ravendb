using System;

namespace Raven.Client
{
	public interface IBulkInsertOperation : IDisposable
	{
		void Store(object entity);
	}
}