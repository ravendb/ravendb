using System;

namespace Raven.Database.Storage
{
	public interface IMultipleCallsBatch
	{
		void Batch(Action<IStorageActionsAccessor> action);
		void Commit();
	}
}