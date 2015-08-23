using System;
using System.Net;
using System.Threading.Tasks;
using FluentAssertions;
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
				e.StatusCode.Should().Be(HttpStatusCode.Forbidden);
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
