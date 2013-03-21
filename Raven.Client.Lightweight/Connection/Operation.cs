using System.Threading;

namespace Raven.Client.Connection
{
	public class Operation
	{
		private readonly ServerClient client;
		private long id;

		public Operation(ServerClient serverClient, long id)
		{
			client = serverClient;
			this.id = id;
		}

		public void WaitForCompletion()
		{
			if(client == null)
				return;

			while (true)
			{
#if !SILVERLIGHT
				var status = client.GetOperationStatus(id);
#else
					var status = client.GetOperationStatusAsync(id).Result;
#endif
				if (status == null)
					break;
				if (status.Value<bool>("Completed"))
					break;
				Thread.Sleep(500);
			}
		}
	}
}
