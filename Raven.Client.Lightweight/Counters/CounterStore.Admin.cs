using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Json.Linq;

namespace Raven.Client.Counters
{
    public partial class CounterStore
    {
		public class CounterStoreAdminOperations
		{
			private readonly CounterStore parent;
			
			internal CounterStoreAdminOperations(CounterStore parent)
			{
				this.parent = parent;
			}			

			public async Task<CounterNameGroupPair[]> GetCounterStorageNameAndGroups(string counterStorageName = null, CancellationToken token = default(CancellationToken))
			{
				parent.AssertInitialized();

				var requestUriString = String.Format("{0}/admin/cs/{1}?op=groups-names", parent.Url, counterStorageName ?? parent.Name);

				using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
					return response.ToObject<CounterNameGroupPair[]>(parent.JsonSerializer);
				}
			}

			public async Task<CounterSummary[]> GetCounterStorageSummary(string counterStorageName = null, CancellationToken token = default(CancellationToken))
			{
				parent.AssertInitialized();

				var requestUriString = String.Format("{0}/admin/cs/{1}?op=summary", parent.Url, counterStorageName ?? parent.Name);

				using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
					return response.ToObject<CounterSummary[]>(parent.JsonSerializer);
				}
			}

			/// <summary>
			/// Create new counter storage on the server.
			/// </summary>
			/// <param name="counterStorageDocument">Settings for the counter storage. If null, default settings will be used, and the name specified in the client ctor will be used</param>
			/// <param name="counterStorageName">Override counter storage name specified in client ctor. If null, the name already specified will be used</param>
			/// <param name="shouldUpateIfExists">If the storage already there, should we update it</param>
			/// <param name="credentials">Credentials used for this operation.</param>
			/// <param name="token">Cancellation token used for this operation.</param>
			public async Task<CounterStore> CreateCounterStorageAsync(CounterStorageDocument counterStorageDocument, 
				string counterStorageName, 
				bool shouldUpateIfExists = false,
				OperationCredentials credentials = null, 
				CancellationToken token = default(CancellationToken))
			{
				if (counterStorageDocument == null)
					throw new ArgumentNullException("counterStorageDocument");

				if (counterStorageName == null) throw new ArgumentNullException("counterStorageName");

				parent.AssertInitialized();

				var urlTemplate = "{0}/admin/cs/{1}";
				if (shouldUpateIfExists)
					urlTemplate += "?update=true";

				var requestUriString = String.Format(urlTemplate, parent.Url, counterStorageName);

				using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Put))
				{
					try
					{
						await request.WriteAsync(RavenJObject.FromObject(counterStorageDocument)).WithCancellation(token).ConfigureAwait(false);
					}
					catch (ErrorResponseException e)
					{
						if (e.StatusCode == HttpStatusCode.Conflict)
							throw new InvalidOperationException("Cannot create counter storage with the name '" + counterStorageName + "' because it already exists. Use shouldUpateIfExists = true flag in case you want to update an existing counter storage", e);

						throw;
					}
				}

				return new CounterStore
				{
					Name = counterStorageName,
					Url = parent.Url,
					Credentials = credentials ?? parent.Credentials
				};
			}

			public async Task DeleteCounterStorageAsync(string counterStorageName, bool hardDelete = false, CancellationToken token = default(CancellationToken))
			{
				parent.AssertInitialized();

				var requestUriString = string.Format("{0}/admin/cs/{1}?hard-delete={2}", parent.Url, counterStorageName, hardDelete);

				using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
				{
					try
					{
						await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
					}
					catch (ErrorResponseException e)
					{
						if (e.StatusCode == HttpStatusCode.NotFound)
							throw new InvalidOperationException(string.Format("Counter storage with specified name ({0}) doesn't exist", counterStorageName));
						throw;
					}
				}
			}

			public async Task<string[]> GetCounterStoragesNamesAsync(CancellationToken token = default(CancellationToken))
			{
				parent.AssertInitialized();

				var requestUriString = String.Format("{0}/cs", parent.Url);

				using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
					return response.ToObject<string[]>(parent.JsonSerializer);
				}
			}
			 
		}
    }
}
