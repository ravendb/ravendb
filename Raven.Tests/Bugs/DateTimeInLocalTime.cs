using System;
using System.Linq;
using Lucene.Net.Documents;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class DateTimeInLocalTime : RavenTest
	{
		[Fact]
		public void CanSaveAndLoadSameTimeLocal()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var serviceExecutionLog = new ServiceExecutionLog
					{
						LastDateChecked = new DateTime(2010, 2, 17, 19, 06, 06,DateTimeKind.Local)
					};

					session.Store(serviceExecutionLog);

					session.SaveChanges();

				}

				using (var session = store.OpenSession())
				{
					var log = session.Load<ServiceExecutionLog>("ServiceExecutionLogs/1");
					Assert.Equal(new DateTime(2010, 2, 17, 19, 06, 06), log.LastDateChecked);
					Assert.Equal(DateTimeKind.Local, log.LastDateChecked.Kind);
				}
			}
		}

		[Fact]
		public void CanSaveAndLoadSameTimeUnspecified()
		{
			using (var store = NewDocumentStore())
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
			using (var store = NewDocumentStore())
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
					Assert.Equal(DateTimeKind.Utc, log.LastDateChecked.Kind);
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