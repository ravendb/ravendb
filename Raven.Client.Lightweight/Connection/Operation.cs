using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public class Operation
	{
		private readonly AsyncServerClient asyncServerClient;
		private readonly long id;
		private readonly RavenJToken state;

		public Operation(long id, RavenJToken state)
		{
			this.id = id;
			this.state = state;
		}

		public Operation(AsyncServerClient asyncServerClient, long id)
		{
			this.asyncServerClient = asyncServerClient;
			this.id = id;
		}


		public async Task<RavenJToken> WaitForCompletionAsync()
		{
			if (asyncServerClient == null)
				return state;

			while (true)
			{
				var status = await asyncServerClient.GetOperationStatusAsync(id);
				if (status == null)
					return null;
				if (status.Value<bool>("Completed"))
					return status.Value<RavenJToken>("State");

#if SILVERLIGHT
				await TaskEx.Delay(500);
#else
				await Task.Delay(500);
#endif

			}
		}

#if !SILVERLIGHT && !NETFX_CORE
		public RavenJToken WaitForCompletion()
		{
			return WaitForCompletionAsync().Result;
		}
#endif

	}
}
