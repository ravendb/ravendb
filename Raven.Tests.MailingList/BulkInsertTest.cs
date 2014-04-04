using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class BulkInsertTest : RavenTestBase
	{
		public class Sample
		{
			public string Name { get; set; }
			public string Id { get; set; }
		}

		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "Versioning";
		}

		[Fact]
		public void BulkInsertFail()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new
					{
						Exclude = false,
						Id = "Raven/Versioning/DefaultConfiguration",
						MaxRevisions = 5
					});
					session.SaveChanges();
				}

				using (var insert = store.BulkInsert())
				{
					insert.Store(new Sample { Name = "Test", Id = "testsample" });
				}

				using (var session = store.OpenSession())
				{
					var sample = session.Load<Sample>("testsample");
					Assert.NotNull(sample);
				}
			}
		}
	}
}
