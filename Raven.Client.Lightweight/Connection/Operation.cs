using System.Threading;
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

#if !SILVERLIGHT && !NETFX_CORE
		private readonly ServerClient client;

		public Operation(ServerClient serverClient, long id)
		{
			client = serverClient;
			this.id = id;
		}
#endif

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
			if (client == null)
				return state;

			while (true)
			{
				var status = client.GetOperationStatus(id);
				if (status == null)
					return null;
				if (status.Value<bool>("Completed"))
					return status.Value<RavenJToken>("State");
				Thread.Sleep(500);
			}
		}
#endif

	}
}
