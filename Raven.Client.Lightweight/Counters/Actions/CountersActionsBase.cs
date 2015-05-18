using System;
using System.Globalization;
using System.Net.Http;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Connection.Implementation;
using Raven.Client.Connection.Profiling;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.Counters.Actions
{
	/// <summary>
	/// implements administration level counters functionality
	/// </summary>
	public abstract class CountersActionsBase : IHoldProfilingInformation
 	{
		private readonly OperationCredentials credentials;
		private readonly HttpJsonRequestFactory jsonRequestFactory;
		private readonly Convention convention;
		protected readonly string ServerUrl;
		protected readonly JsonSerializer JsonSerializer;
		protected readonly string CounterStorageUrl;
		protected readonly ICounterStore Parent;
		protected readonly string CounterStorageName;

		public ProfilingInformation ProfilingInformation { get; private set; } //so far it is preparation for air conditioning

		protected CountersActionsBase(ICounterStore parent, string counterStorageName)
		{
			credentials = parent.Credentials;
			jsonRequestFactory = parent.JsonRequestFactory;
			ServerUrl = parent.Url;
			this.Parent = parent;
			this.CounterStorageName = counterStorageName;
			CounterStorageUrl = string.Format(CultureInfo.InvariantCulture, "{0}/cs/{1}", ServerUrl, counterStorageName);
			JsonSerializer = parent.JsonSerializer;
			convention = parent.Convention;
			ProfilingInformation = parent.ProfilingInformation;
		}

		protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, HttpMethod httpMethod, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
		{
			CreateHttpJsonRequestParams @params;
			if (timeout.HasValue)
			{
				@params = new CreateHttpJsonRequestParams(this, requestUriString, httpMethod, credentials, convention)
				{
					DisableRequestCompression = disableRequestCompression,
					DisableAuthentication = disableAuthentication,
					Timeout = timeout.Value
				};
			}
			else
			{
				@params = new CreateHttpJsonRequestParams(this, requestUriString, httpMethod, credentials, convention)
				{
					DisableRequestCompression = disableRequestCompression,
					DisableAuthentication = disableAuthentication,
				};				
			}
			var request = jsonRequestFactory.CreateHttpJsonRequest(@params);
		
			return request;
		}
	}

}