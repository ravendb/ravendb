using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;

namespace Raven.Client.Connection
{
	public class Operation
	{
		private readonly AsyncServerClient asyncServerClient;
		private readonly long id;
		
#if !SILVERLIGHT
		private readonly ServerClient client;

		public Operation(ServerClient serverClient, long id)
		{
			client = serverClient;
			this.id = id;
		}
#endif

		public Operation(long id)
		{
			this.id = id;
		}

		public Operation(AsyncServerClient asyncServerClient, long id)
		{
			this.asyncServerClient = asyncServerClient;
			this.id = id;
		}


		public async Task WaitForCompletionAsync()
		{
			if (asyncServerClient == null)
				return;

			while (true)
			{
				var status = await asyncServerClient.GetOperationStatusAsync(id);
				if (status == null)
					break;
				if (status.Value<bool>("Completed"))
					break;
				await TaskEx.Delay(500);
			}
		}

#if !SILVERLIGHT
		public void WaitForCompletion()
		{
			if(client == null)
				return;

			while (true)
			{
				var status = client.GetOperationStatus(id);
				if (status == null)
					break;
				if (status.Value<bool>("Completed"))
					break;
				Thread.Sleep(500);
			}
		}
#endif

	}
}
