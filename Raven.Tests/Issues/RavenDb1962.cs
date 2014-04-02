using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Bugs;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDb1962 : RavenTest
	{
		private const int Cntr = 5;

		[Fact]
		public async Task CanDisplayLazyValues()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					await StoreDataAsync(store, session);

					var userFetchTasks = LazyLoadAsync(store, session);
					var i = 1;
					foreach (var lazy in userFetchTasks)
					{
						var user = await lazy.Value;
						Assert.Equal(user.Name, "Test User #" + i);
						i++;
					}
				}
			}
		}

		[Fact]
		public async Task CanDisplayLazyRequestTimes_RemoteAndData()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					await StoreDataAsync(store, session);
				}

				using (var session = store.OpenAsyncSession())
				{
					
					LazyLoadAsync(store, session);

					var requestTimes = await session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
					Assert.NotNull(requestTimes.TotalClientDuration);
					Assert.NotNull(requestTimes.TotalServerDuration);
					Assert.Equal(Cntr, requestTimes.DurationBreakdown.Count);
				}

				using (var ses = store.OpenSession())
				{
					var queryRes = ses.Query<User>().Where(x => x.Name.StartsWith("T")).ToList();
					var i = 1;
					foreach (var res in queryRes)
					{
						Assert.Equal(res.Name, "Test User #" + i);
						i++;
					}
				}
			}
		}


		public async Task StoreDataAsync(DocumentStore store, IAsyncDocumentSession session)
		{
			for (var i = 1; i <= Cntr; i++)
			{
				await session.StoreAsync(new User {Name = "Test User #" + i}, "users/" + i);
			}
			await session.SaveChangesAsync();
		}

		public List<Lazy<Task<User>>> LazyLoadAsync(DocumentStore store, IAsyncDocumentSession session)
		{
			var listTasks = new List<Lazy<Task<User>>>();
			for (var i = 1; i <= Cntr; i++)
			{
				var userFetchTask = session.Advanced.Lazily.LoadAsync<User>("users/" + i);

				listTasks.Add(userFetchTask);
			}
			return listTasks;
		}
	}
}