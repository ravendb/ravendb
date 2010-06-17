using System;
using System.Collections.Generic;

namespace Raven.Database.Storage.StorageActions
{
	public interface IQueueStorageActions
	{
		void EnqueueToQueue(string name, byte[] data);
		IEnumerable<Tuple<byte[], int>> PeekFromQueue(string name);
		void DeleteFromQueue(int id);
	}
}