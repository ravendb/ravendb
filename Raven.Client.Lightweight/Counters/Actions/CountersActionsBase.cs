using System;
using System.Globalization;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.Counters.Actions
{
	/// <summary>
	/// implements administration level counters functionality
	/// </summary>
	public abstract class CountersActionsBase : IHoldProfilingInformation
 	{
		protected readonly OperationCredentials credentials;
		protected readonly HttpJsonRequestFactory jsonRequestFactory;
		protected readonly string serverUrl;
		protected readonly string counterName;
		protected readonly Convention convention;
		protected readonly JsonSerializer jsonSerializer;
		protected readonly string counterStorageUrl;

		public ProfilingInformation ProfilingInformation { get; private set; } //so far it is preparation for air conditioning

		protected CountersActionsBase(ICounterStore parent,string counterName)
		{
			credentials = parent.Credentials;
			jsonRequestFactory = parent.JsonRequestFactory;
			serverUrl = parent.Url;
			this.counterName = counterName;
			counterStorageUrl = string.Format(CultureInfo.InvariantCulture, "{0}/cs/{1}", serverUrl, counterName);
			jsonSerializer = parent.JsonSerializer;
			convention = parent.Convention;
			ProfilingInformation = parent.ProfilingInformation;
		}

		protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, string httpVerb, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
		{
			CreateHttpJsonRequestParams @params;
			if (timeout.HasValue)
			{
				@params = new CreateHttpJsonRequestParams(this, requestUriString, httpVerb, credentials, convention)
				{
					DisableRequestCompression = disableRequestCompression,
					DisableAuthentication = disableAuthentication,
					Timeout = timeout.Value
				};
			}
			else
			{
				@params = new CreateHttpJsonRequestParams(this, requestUriString, httpVerb, credentials, convention)
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