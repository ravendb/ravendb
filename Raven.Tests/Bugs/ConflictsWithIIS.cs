using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Bugs.Identifiers;

namespace Raven.Tests.Bugs
{
	public class ConflictsWithIIS : IisExpressTestClient
	{
		public class DeviceStatusRecord
		{
			public int DeviceId { get; set; }
			public DateTimeOffset Timestamp { get; set; }
			public int StatusId { get; set; }
		}

		[IISExpressInstalledFact]
		public void MultiThreadedInsert()
		{
			const int threadCount = 4;
			var tasks = new List<Task>();

			using (var store = NewDocumentStore())
			{
				for (int i = 1; i <= threadCount; i++)
				{
					var copy = i;
					var taskHandle = Task.Factory.StartNew(() => DoInsert(store, copy));
					tasks.Add(taskHandle);
				}

				Task.WaitAll(tasks.ToArray());
			}
		}

		[IISExpressInstalledFact]
		public void InnefficientMultiThreadedInsert()
		{
			const int threadCount = 4;
			var tasks = new List<Task>();

			using (var store = NewDocumentStore())
			{
				for (int i = 1; i <= threadCount; i++)
				{
					var copy = i;
					var taskHandle = Task.Factory.StartNew(() => DoInefficientInsert(store.Url, copy));
					tasks.Add(taskHandle);
				}

				Task.WaitAll(tasks.ToArray());
			}
		}

		private void DoInsert(IDocumentStore store, int deviceId)
		{
			using (var session = store.OpenSession())
			{
				session.Store(new DeviceStatusRecord
								  {
									  DeviceId = deviceId,
									  Timestamp = DateTime.Now,
									  StatusId = 1024
								  });
				session.SaveChanges();
			}
		}

		private void DoInefficientInsert(string url, int deviceId)
		{
			using (var store = new DocumentStore { Url = url }.Initialize())
			using (var session = store.OpenSession())
			{
				session.Store(new DeviceStatusRecord
								  {
									  DeviceId = deviceId,
									  Timestamp = DateTime.Now,
									  StatusId = 1024
								  });
				session.SaveChanges();
			}
		}
	}
}
