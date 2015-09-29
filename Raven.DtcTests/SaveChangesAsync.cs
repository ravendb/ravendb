// -----------------------------------------------------------------------
//  <copyright file="SaveChangesAsync.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using System.Transactions;
using Raven.Tests.Bugs;
using Raven.Tests.Common;
using Xunit;

namespace Raven.DtcTests
{
	public class SaveChangesAsync : RavenTest
	{
		[Fact]
		public async Task Should_just_work()
		{
			using (var documentStore = NewDocumentStore(requestedStorage: "esent"))
			{
				EnsureDtcIsSupported(documentStore);

				using (var s = documentStore.OpenAsyncSession())
				{
					await s.StoreAsync(new AccurateCount.User { Name = "Ayende" });
					await s.SaveChangesAsync();
				}

				using (var s = documentStore.OpenAsyncSession())
				using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
				{
					var user = await s.LoadAsync<AccurateCount.User>("users/1");
					user.Name = "Rahien";
					await s.SaveChangesAsync();
					scope.Complete();
				}

				using (var s = documentStore.OpenAsyncSession())
				{
					s.Advanced.AllowNonAuthoritativeInformation = false;
					var user = await s.LoadAsync<AccurateCount.User>("users/1");
					Assert.Equal("Rahien", user.Name);
				}
			}
		}
	}
}