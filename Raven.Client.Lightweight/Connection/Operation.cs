using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;

namespace Raven.Client.Connection
{
	public class Operation
	{
		private readonly AsyncServerClient asyncServerClient;

		private readonly long id;

		public Operation(long id, AsyncServerClient asyncServerClient)
		{
			this.asyncServerClient = asyncServerClient;
			this.id = id;
		}

#if !SILVERLIGHT
		public void WaitForCompletion()
		{
			WaitForCompletionAsync().Wait();
		}


#endif

		public async Task WaitForCompletionAsync()
		{
			if (asyncServerClient == null)
			{
				return;
			}

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

	}
}
