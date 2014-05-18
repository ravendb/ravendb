using System.Transactions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Asger : RavenTest
	{
		[Fact]
		public void putting_and_patching_in_same_transaction()
		{
            using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				Assert.DoesNotThrow(() =>
				{
					using (var tx = new TransactionScope())
					{
						store.DatabaseCommands.Batch(new ICommandData[]
						{
							new PutCommandData
							{
								Key = "RebusSubscriptions/TheMessage",
								Metadata = new RavenJObject
								{
									{
						                             	"Raven-Entity-Name",
						                             	"RebusSubscriptions"
						                             	},
								},
								Document = new RavenJObject
								{
									{"Endpoints", new RavenJArray()}
								}
							},
							new PatchCommandData
							{
								Key = "RebusSubscriptions/TheMessage",
								Patches = new[]
								{
									new PatchRequest
									{
										Type = PatchCommandType.Add,
										Name = "Endpoints",
										Value = new
						                             	RavenJValue("application_queue"),
									}
								}
							}
						});

						tx.Complete();
					}
				});
			}
		}
	}
}