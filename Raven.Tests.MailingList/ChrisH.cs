using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class ChrisH : RavenTest
	{
		[Fact]
		public void CanReadStrangeId()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new { Name = "Oren"}, "__1.1.2.3_e$_Program Files_Hurricane MTA Server_accounts_10_logfiles_20120502Processed.log");
					session.SaveChanges();
				}

				using(var session = store.OpenSession())
				{
					var load = session.Load<dynamic>("__1.1.2.3_e$_Program Files_Hurricane MTA Server_accounts_10_logfiles_20120502Processed.log");
					Assert.Equal("Oren", load.Name);

					session.SaveChanges();
				}

			}
		}

		[Fact]
		public void CanReadStrangeId_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new { Name = "Oren" }, "__1.1.2.3_e$_Program Files_Hurricane MTA Server_accounts_10_logfiles_20120502Processed.log");
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var load = session.Load<dynamic>("__1.1.2.3_e$_Program Files_Hurricane MTA Server_accounts_10_logfiles_20120502Processed.log");
					Assert.Equal("Oren", load.Name);

					session.SaveChanges();
				}

			}
		}
	}
}