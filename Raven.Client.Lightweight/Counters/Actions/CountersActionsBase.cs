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
		internal class Verbs
		{
			internal const string Put = "PUT";
			internal const string Post = "POST";
			internal const string Get = "GET";
			internal const string Delete = "DELETE";
			internal const string Head = "HEAD";
		}

		protected readonly OperationCredentials credentials;
		protected readonly HttpJsonRequestFactory jsonRequestFactory;
		protected readonly string serverUrl;
		protected readonly string counterStorageName;
		protected readonly Convention convention;
		protected readonly JsonSerializer jsonSerializer;
		protected readonly string counterStorageUrl;

		public ProfilingInformation ProfilingInformation { get; private set; } //so far it is preparation for air conditioning

		protected CountersActionsBase(ICounterStore parent,string counterName)
		{
			credentials = parent.Credentials;
			jsonRequestFactory = parent.JsonRequestFactory;
			serverUrl = parent.Url;
			counterStorageName = counterName;
			counterStorageUrl = string.Format(CultureInfo.InvariantCulture, "{0}/cs/{1}", serverUrl, counterName);
			jsonSerializer = parent.JsonSerializer;
			convention = parent.Convention;
			ProfilingInformation = parent.ProfilingInformation;
		}

		protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, string httpVerb, bool disableRequestCompression = false, bool disableAuthentication = false)
		{
			return jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUriString, httpVerb, credentials, convention)
			{
				DisableRequestCompression = disableRequestCompression,
				DisableAuthentication = disableAuthentication
			});
		}
	}
}