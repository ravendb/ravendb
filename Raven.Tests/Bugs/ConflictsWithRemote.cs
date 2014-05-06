using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class ConflictsWithRemote: RavenTest
	{
		public class DeviceStatusRecord
		{
			public int DeviceId { get; set; }
			public DateTimeOffset Timestamp { get; set; }
			public int StatusId { get; set; }
		}

        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.DefaultStorageTypeName = "esent";
            configuration.RunInMemory = false;
        }


		[Fact]
		//[TimeBombedFact(2013, 12, 31)]
		public void MultiThreadedInsert()
		{
			const int threadCount = 4;
			var tasks = new List<Task>();

			using(var server = GetNewServer())
			using (var store = new DocumentStore{Url = server.SystemDatabase.Configuration.ServerUrl}.Initialize())
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

		[Fact]
		public void InnefficientMultiThreadedInsert()
		{
			const int threadCount = 4;
			var tasks = new List<Task>();

			using (var server = GetNewServer())
			{
				for (int i = 1; i <= threadCount; i++)
				{
					var copy = i;
					var taskHandle = Task.Factory.StartNew(() => DoInefficientInsert(server.SystemDatabase.Configuration.ServerUrl, copy));
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