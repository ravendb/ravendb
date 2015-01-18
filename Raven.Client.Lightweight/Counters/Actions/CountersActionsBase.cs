using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.Counters.Actions
{
	/// <summary>
	/// implements administration level counters functionality
	/// </summary>
	public abstract class CountersActionsBase
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
		protected readonly string defaultStorageName;
		protected readonly CountersClient parent;
		protected readonly Convention convention;
		protected readonly JsonSerializer jsonSerializer;
		protected readonly string counterStorageUrl;

		public ProfilingInformation ProfilingInformation { get; private set; } //so far it is preparation for air conditioning

		protected CountersActionsBase(CountersClient parent)
		{
			credentials = parent.PrimaryCredentials;
			jsonRequestFactory = parent.JsonRequestFactory;
			serverUrl = parent.ServerUrl;
			defaultStorageName = parent.DefaultStorageName;
			counterStorageUrl = parent.CounterStorageUrl;
			jsonSerializer = parent.JsonSerializer;
			convention = parent.Conventions;
			this.parent = parent;
		}

		protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, string httpVerb, bool disableRequestCompression = false, bool disableAuthentication = false)
		{
			return jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(parent, requestUriString, httpVerb, credentials, convention)
			{
				DisableRequestCompression = disableRequestCompression,
				DisableAuthentication = disableAuthentication
			});
		}
	}
}