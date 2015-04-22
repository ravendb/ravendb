using System;
using System.Globalization;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Encryption;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3117 : Encryption
	{
		[Fact]
		public void Query_on_encrypted_index_should_work()
		{
			const string IndexName = "TestIndex";

            var ravenDbServer = GetServer();

			ravenDbServer.SystemDatabase.Indexes.PutIndex(IndexName,
				new IndexDefinition
				{
					Map =
						@"
							from doc in docs
							let expiry = doc[""@metadata""][""Raven-Expiration-Date""]
							where expiry != null
							select new { Expiry = expiry }
						"
				});

			var currentTime = SystemTime.UtcNow;
			var nowAsStr = currentTime.ToString(Default.DateTimeFormatsToWrite, CultureInfo.InvariantCulture);
			ravenDbServer.SystemDatabase.Documents.Put("foo/1", null,
				RavenJObject.FromObject(new User { Name = "FooBar" }),
				RavenJObject.Parse("{ \"Raven-Expiration-Date\" : \"" + nowAsStr + "\"}"), null);
			WaitForIndexing(ravenDbServer.SystemDatabase);

			using (ravenDbServer.SystemDatabase.DisableAllTriggersForCurrentThread())
			{
				var query = "Expiry:[* TO " + nowAsStr + "]";

				var queryResult = ravenDbServer.SystemDatabase.Queries.Query(IndexName, new IndexQuery
				{
					Start = 0,
					PageSize = 512,
					Cutoff = currentTime,
					Query = query,
					FieldsToFetch = new[] { "__document_id" }
				}, CancellationToken.None);

				Assert.True(queryResult.Results.Count > 0);
			}
		}

		public class User
		{
			public string Name { get; set; }
		}
	}
}
