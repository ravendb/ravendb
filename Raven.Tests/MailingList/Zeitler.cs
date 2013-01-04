using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Zeitler : RavenTest
	{
		public class PersistentCacheKey
		{
			public string Id { get; set; }
			public byte[] Hash { get; set; }
			public string RoutePattern { get; set; }
			public string ETag { get; set; }
			public DateTimeOffset LastModified { get; set; }
		}
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.RunInMemory = false;
		}
		[Fact]
		public void AddTest()
		{
			// want a green test? comment this	
			using (var documentStore = NewDocumentStore())
			{
				documentStore.Initialize();
				new RavenDocumentsByEntityName().Execute(documentStore);


				// want a green test? uncomment this	
				//var documentStore = new DocumentStore() {
				//	Url = "http://localhost:8082/databases/entitytagstore"
				//}.Initialize();

				byte[] hash;

				using (var sha1 = new SHA1CryptoServiceProvider())
				{
					hash = sha1.ComputeHash(Encoding.UTF8.GetBytes("/api/Cars"));
				}

				var persistentCacheKey = new PersistentCacheKey()
				{
					ETag = "\"abcdef1234\"",
					Hash = hash,
					LastModified = DateTime.Now,
					RoutePattern = "/api/Cars"
				};

				using (var session = documentStore.OpenSession())
				{
					session.Store(persistentCacheKey);
					session.SaveChanges();
				}
				PersistentCacheKey key;
				using (var session = documentStore.OpenSession())
				{
					key = session.Query<PersistentCacheKey>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.FirstOrDefault(p => p.Hash == hash);
				}

				Assert.NotNull(key);
			}
		}
	}
}