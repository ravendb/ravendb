using System;

namespace Raven.Client
{
	public interface IBulkInsertOperation : IDisposable
	{
		void Add(object entity);
	}
}