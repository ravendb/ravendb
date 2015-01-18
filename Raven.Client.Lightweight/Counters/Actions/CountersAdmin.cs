using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Actions
{
	/// <summary>
	/// implements administration level counters functionality
	/// </summary>
	public class CountersAdmin : CountersActionsBase
	{
		internal CountersAdmin(CountersClient parent) : base(parent)
		{

		}

		public async Task<string[]> GetCounterStoragesNamesAsync(CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/counterStorage/conterStorages", serverUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<string[]>(jsonSerializer);
			}
		}

		public async Task<List<CounterStorageStats>> GetCounterStoragesStatsAsync(CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/counterStorage/stats", serverUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<List<CounterStorageStats>>(jsonSerializer);
			}
		}

		/// <summary>
		/// Create new counter storage on the server.
		/// </summary>
		/// <param name="countersDocument">Settings for the counter storage. If null, default settings will be used, and the name specified in the client ctor will be used</param>
		/// <param name="counterName">Override counter storage name specified in client ctor. If null, the name already specified will be used</param>
		public async Task CreateCounterStorageAsync(CountersDocument countersDocument, string counterName = null, CancellationToken token = default(CancellationToken))
		{
			if (countersDocument == null)
				throw new ArgumentNullException("countersDocument");

			counterName = counterName ?? defaultStorageName;
			var requestUriString = String.Format("{0}/counterstorage/admin/{1}", serverUrl, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Put))
			{
				try
				{
					await request.WriteAsync(RavenJObject.FromObject(countersDocument)).WithCancellation(token).ConfigureAwait(false);
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.Conflict)
						throw new InvalidOperationException("Cannot create counter storage with the name '" + counterName + "' because it already exists. Use CreateOrUpdateCounterStorageAsync in case you want to update an existing counter storage", e);

					throw;
				}					
			}
		}

		/// <summary>
		/// Create new counter storage on the server or update existing one.
		/// </summary>
		public async Task CreateOrUpdateCounterStorageAsync(CountersDocument countersDocument, string counterName = null, CancellationToken token = default(CancellationToken))
		{
			if (countersDocument == null)
				throw new ArgumentNullException("countersDocument");

			counterName = counterName ?? defaultStorageName;
			var requestUriString = String.Format("{0}/counterstorage/admin/{1}?update=true", serverUrl, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Put))
				await request.WriteAsync(RavenJObject.FromObject(countersDocument)).WithCancellation(token).ConfigureAwait(false);
		}

		public async Task DeleteCounterStorageAsync(string counterName = null, bool hardDelete = false, CancellationToken token = default(CancellationToken))
		{
			counterName = counterName ?? defaultStorageName;
			var requestUriString = String.Format("{0}/counterstorage/admin/{1}?hard-delete={2}", serverUrl, counterName, hardDelete);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Delete))
			{
				try
				{
					await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException(string.Format("Counter storage with specified name ({0}) doesn't exist", counterName));
					throw;
				}
			}
		}
	}
}