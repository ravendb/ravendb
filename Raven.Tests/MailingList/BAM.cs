using System;
using Raven.Client.Document;
using Xunit;
using Raven.Client.Extensions;

namespace Raven.Tests.MailingList
{
	public class BAM : LocalClientTest
	{
		[Fact]
		public void get_dbnames_test()
		{
			using (var server = GetNewServer())
			using (var docStore = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
			{
				var dbNames = docStore.DatabaseCommands.GetDatabaseNames(25, 0);

				Assert.Empty(dbNames);

				docStore.DatabaseCommands.EnsureDatabaseExists("test");

				dbNames = docStore.DatabaseCommands.GetDatabaseNames(25, 0);

				Assert.NotEmpty(dbNames);

			}
		}



		[Fact]
		public void id_with_backslash_remote()
		{
			var goodId = "good/one";
			var badId = @"bad\one";

			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					var goodIn = new {Id = goodId};
					session.Store(goodIn);

					var badIn = new {Id = badId};
					session.Store(badIn);

					var throws = Assert.Throws<InvalidOperationException>(()=>session.SaveChanges());

					Assert.Contains(@"PUT vetoed by Raven.Database.Plugins.Builtins.InvalidDocumentNames because: Document names cannot contains '\' but attempted to save with: bad\one", throws.Message);
				}
			}
		}



		[Fact]
		public void Cannot_create_tenant_named_default()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var throws = Assert.Throws<InvalidOperationException>(() => store.DatabaseCommands.EnsureDatabaseExists("default"));

				Assert.Contains(@"Cannot create a tenant database with the name 'default', that name is reserved for the actual default database", throws.Message);
		
			}
		}
	}
}
