using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Counters;
using Raven.Database.Config;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CounterAuthTests : RavenBaseCountersTest
	{
		private const string GoodApiKey = "thisIsApiKeyName/thisIsSecret";
		private const string BadApiKey = "NotThisIsApiKeyName/thisIsSecret";

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			ConfigureServerForAuth(configuration);
		}

		[Fact]
		public void Cannot_use_admin_endpoint_with_non_admin_apiKey()
		{
			string storeName;
			using (var store = NewRemoteCountersStore("TestStore")) //this will create the TestStore
			{
				storeName = store.Name;
			}

			Database.Server.Security.Authentication.EnableOnce();

			//GoodApiKey is with admin access to <system>
			ConfigureApiKey(servers[0].SystemDatabase, "thisIsApiKeyName", "thisIsSecret", storeName, true);

			//BadApiKey is without admin access to <system>
			ConfigureApiKey(servers[0].SystemDatabase, "NotThisIsApiKeyName", "thisIsSecret", storeName);

			using (var store = new CounterStore
			{
				Url = servers[0].SystemDatabase.ServerUrl,
				Name = storeName,
				Credentials = new OperationCredentials(BadApiKey, null)
			})
			{
				store.Initialize();
				var e = Assert.Throws<ErrorResponseException>(() => AsyncHelpers.RunSync(() => store.Admin.GetCounterStoragesNamesAsync()));
				
                Assert.Equal(HttpStatusCode.Forbidden, e.StatusCode);
			}

			using (var store = new CounterStore
			{
				Url = servers[0].SystemDatabase.ServerUrl,
				Name = storeName,
				Credentials = new OperationCredentials(GoodApiKey, null)
			})
			{
				store.Initialize();
				var storageNames = new string[0];
				Assert.DoesNotThrow(() => storageNames = AsyncHelpers.RunSync(() => store.Admin.GetCounterStoragesNamesAsync()));
				
                Assert.Equal(1, storageNames.Length);
			}
		}

		[Fact]
		public async Task When_Counter_has_apiKey_auth_then_operations_with_wrong_apiKey_should_fail()
		{
			String storeName;
			using (var store = NewRemoteCountersStore("TestStore")) //this will create the TestStore
			{
				storeName = store.Name;
				await store.IncrementAsync("G", "C");
			}

			Database.Server.Security.Authentication.EnableOnce();
			ConfigureApiKey(servers[0].SystemDatabase, "thisIsApiKeyName", "thisIsSecret", storeName);
			ConfigureApiKey(servers[0].SystemDatabase, "NotThisIsApiKeyName", "thisIsSecret", "NonExistingResourceName");

			using (var store = new CounterStore
			{
				Url = servers[0].SystemDatabase.ServerUrl,
				Name = storeName,
				Credentials = new OperationCredentials(BadApiKey,null)
			})
			{
				store.Initialize();
				var e = Assert.Throws<ErrorResponseException>(() => AsyncHelpers.RunSync(() => store.IncrementAsync("G", "C")));

                Assert.Equal(HttpStatusCode.Forbidden, e.StatusCode);
            }

			using (var store = new CounterStore
			{
				Url = servers[0].SystemDatabase.ServerUrl,
				Name = storeName,
				Credentials = new OperationCredentials(GoodApiKey, null)
			})
			{
				store.Initialize();
				Assert.DoesNotThrow(() => AsyncHelpers.RunSync(() => store.IncrementAsync("G", "C")));
			}
		}
	}
}
