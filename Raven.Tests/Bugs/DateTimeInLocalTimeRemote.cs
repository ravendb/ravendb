using System;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class DateTimeInLocalTimeRemote : RavenTest
	{
		[Fact]
		public void CanSaveAndLoadSameTimeLocal()
		{
			using(GetNewServer())
			using (var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var serviceExecutionLog = new ServiceExecutionLog
					{
						LastDateChecked = new DateTime(2010, 2, 17, 19, 06, 06, DateTimeKind.Local)
					};

					session.Store(serviceExecutionLog);

					session.SaveChanges();

				}

				using (var session = store.OpenSession())
				{
					var log = session.Load<ServiceExecutionLog>("ServiceExecutionLogs/1");
					Assert.Equal(new DateTime(2010, 2, 17, 19, 06, 06), log.LastDateChecked);
				}
			}
		}

		[Fact]
		public void CanSaveAndLoadSameTimeUnspecified()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var serviceExecutionLog = new ServiceExecutionLog
					{
						LastDateChecked = new DateTime(2010, 2, 17, 19, 06, 06, DateTimeKind.Unspecified)
					};

					session.Store(serviceExecutionLog);

					session.SaveChanges();

				}

				using (var session = store.OpenSession())
				{
					var log = session.Load<ServiceExecutionLog>("ServiceExecutionLogs/1");
					Assert.Equal(new DateTime(2010, 2, 17, 19, 06, 06), log.LastDateChecked);
				}
			}
		}

		[Fact]
		public void CanSaveAndLoadSameTimeUtc()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var serviceExecutionLog = new ServiceExecutionLog
					{
						LastDateChecked = new DateTime(2010, 2, 17, 19, 06, 06, DateTimeKind.Utc)
					};

					session.Store(serviceExecutionLog);

					session.SaveChanges();

				}

				using (var session = store.OpenSession())
				{
					var log = session.Load<ServiceExecutionLog>("ServiceExecutionLogs/1");
					Assert.Equal(new DateTime(2010, 2, 17, 19, 06, 06), log.LastDateChecked);
				}
			}
		}

		public class ServiceExecutionLog
		{
			public DateTime LastDateChecked { get; set; }
			public string Id { get; set; }
		}
	}
}