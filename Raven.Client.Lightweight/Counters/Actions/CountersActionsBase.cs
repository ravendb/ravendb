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
		protected readonly OperationCredentials credentials;
		protected readonly HttpJsonRequestFactory jsonRequestFactory;
		protected readonly string serverUrl;
		protected readonly string counterStorageName;
		protected readonly CountersClient parent;
		protected readonly Convention convention;
		protected readonly JsonSerializer jsonSerializer;
		protected readonly string counterStorageUrl;

		public ProfilingInformation ProfilingInformation { get; private set; } //so far it is preparation for air conditioning

		protected CountersActionsBase(CountersClient parent, Convention convention)
		{
			credentials = parent.PrimaryCredentials;
			jsonRequestFactory = parent.JsonRequestFactory;
			serverUrl = parent.ServerUrl;
			counterStorageName = parent.CounterStorageName;
			counterStorageUrl = parent.CounterStorageUrl;
			jsonSerializer = parent.JsonSerializer;
			
			this.convention = convention;
			this.parent = parent;
		}
	}
}