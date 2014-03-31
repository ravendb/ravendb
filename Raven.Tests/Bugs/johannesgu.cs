using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class johannesgu : RavenTest
	{
		 [Fact]
		 public void FailureToCommitDocWithSlashInIt()
		 {
			 using(var store = NewDocumentStore())
			 {
				 var tx = new TransactionInformation
				 {
                     Id = Guid.NewGuid().ToString(),
					 Timeout = TimeSpan.FromMinutes(1)
				 };

				 Assert.Throws<OperationVetoedException>(
					 () => store.DocumentDatabase.Documents.Put(@"somebadid\123", null, new RavenJObject(), new RavenJObject(), tx));
			 }
		 }
	}
}