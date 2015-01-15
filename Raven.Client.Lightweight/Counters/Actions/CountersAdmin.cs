using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Actions
{
	/// <summary>
	/// implements administration level counters functionality
	/// </summary>
	public class CountersAdmin
	{
		private readonly OperationCredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly string serverUrl;
		private readonly string counterStorageName;
		private readonly CountersClient _parent;
		private readonly Convention convention;
		private readonly JsonSerializer _jsonSerializer;

		internal CountersAdmin(CountersClient parent, Convention convention)
		{
			credentials = parent.PrimaryCredentials;
			jsonRequestFactory = parent.JsonRequestFactory;
			serverUrl = parent.ServerUrl;
			counterStorageName = parent.CounterStorageName;
			_parent = parent;
			this.convention = convention;
			_jsonSerializer = parent.JsonSerializer;
		}

		public ProfilingInformation ProfilingInformation { get; private set; }

		public async Task<string[]> GetCounterStoragesNames()
		{
			var requestUriString = String.Format("{0}/counterStorage/conterStorages", serverUrl);

			using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "GET", credentials, convention)))
			{
				var response = await request.ReadResponseJsonAsync();
				return response.ToObject<string[]>(_jsonSerializer);
			}
		}

		public async Task<List<CounterStorageStats>> GetCounterStoragesStats()
		{
			var requestUriString = String.Format("{0}/counterStorage/stats", serverUrl);

			using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "GET", credentials, convention)))
			{
				var response = await request.ReadResponseJsonAsync();
				return response.ToObject<List<CounterStorageStats>>(_jsonSerializer);
			}
		}

		/// <summary>
		/// Create new counter storage on the server.
		/// </summary>
		/// <param name="countersDocument">Settings for the counter storage. If null, default settings will be used, and the name specified in the client ctor will be used</param>
		/// <param name="storageName">Override counter storage name specified in client ctor. If null, the name already specified will be used</param>
		public async Task CreateCounterStorageAsync(CountersDocument countersDocument = null, string storageName = null)
		{
			storageName = storageName ?? counterStorageName;
			var requestUriString = String.Format("{0}/counterstorage/admin/{1}", serverUrl, storageName);

			using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "PUT", credentials, convention)))
			{
				try
				{
					await request.WriteAsync(RavenJObject.FromObject(countersDocument ?? new CountersDocument()));
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.Conflict)
						throw new InvalidOperationException("Cannot create counter storage with the name '" + storageName + "' because it already exists. Use CreateOrUpdateCounterStorageAsync in case you want to update an existing counter storage", e);

					throw;
				}					
			}
		}

		/// <summary>
		/// Create new counter storage on the server or update existing one.
		/// </summary>
		public async Task CreateOrUpdateCounterStorageAsync(CountersDocument countersDocument, string storageName = null)
		{
			storageName = storageName ?? counterStorageName;
			var requestUriString = String.Format("{0}/counterstorage/admin/{1}?update=true", serverUrl, storageName);

			using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "PUT", credentials, convention)))
				await request.WriteAsync(RavenJObject.FromObject(countersDocument));
		}

		public async Task DeleteCounterStorageAsync(string storageName = null, bool hardDelete = false)
		{
			storageName = storageName ?? counterStorageName;
			var requestUriString = String.Format("{0}/counterstorage/admin/{1}?hard-delete={2}", serverUrl,
				storageName, hardDelete);

			using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(_parent, requestUriString, "DELETE", credentials, convention)))
			{
				try
				{
					await request.ExecuteRequestAsync();
				}
				catch (ErrorResponseException e)
				{
					if (e.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException(string.Format("Counter storage with specified name ({0}) doesn't exist", storageName));
					throw;
					//throw e.TryThrowBetterError();
				}
			}
		}
	}
}