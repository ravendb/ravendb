using System;
using System.Threading;
using System.Transactions;
using Raven.Json.Linq;
using Raven.Client.Document;
using Xunit;
using TransactionInformation = Raven.Abstractions.Data.TransactionInformation;

namespace Raven.Tests.Bugs.DTC
{
	public class UsingDtcForDelete : RavenTest
	{
		private string documentKey;

		[Fact]
		public void ShouldWork()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				documentKey = "tester123";

				var transactionInformation = new TransactionInformation
				{
					Id = Guid.NewGuid()
				};

				store.DocumentDatabase.Put(documentKey, null, new RavenJObject(),
				                     RavenJObject.Parse(
				                     	@"{
  ""Raven-Entity-Name"": ""MySagaDatas"",
  ""Raven-Clr-Type"": ""TestNServiceBusSagaWithRavenDb.MySagaData, TestNServiceBusSagaWithRavenDb"",
  ""Last-Modified"": ""Mon, 21 Mar 2011 19:59:58 GMT"",
  ""Non-Authoritative-Information"": false
}"), transactionInformation);
				store.DatabaseCommands.Commit(transactionInformation.Id);


				var deleteTx = new TransactionInformation
				{
					Id = Guid.NewGuid()
				};
				store.DocumentDatabase.Delete(documentKey, null, deleteTx);

				store.DocumentDatabase.Commit(deleteTx.Id);
			}
		}
	}
}
