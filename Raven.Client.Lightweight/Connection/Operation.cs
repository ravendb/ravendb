using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public class Operation
	{
		private readonly AsyncServerClient asyncServerClient;
		private readonly long id;
		private readonly RavenJToken state;
		private readonly bool done;

		public Operation(long id, RavenJToken state)
		{
			this.id = id;
			this.state = state;
			this.done = true;
		}

		public Operation(AsyncServerClient asyncServerClient, long id)
		{
			this.asyncServerClient = asyncServerClient;
			this.id = id;
		}


		public async Task<RavenJToken> WaitForCompletionAsync()
		{
			if (done)
				return state;
			if (asyncServerClient == null)
				throw new InvalidOperationException("Cannot use WaitForCompletionAsync() when the operation was executed syncronously");

			while (true)
			{
                var status = await asyncServerClient.GetOperationStatusAsync(id).ConfigureAwait(false);
				if (status == null)
					return null;

				if (status.Value<bool>("Completed"))
				{
					var faulted = status.Value<bool>("Faulted");
					if (faulted)
					{
						var error = status.Value<RavenJObject>("State");
						var errorMessage = error.Value<string>("Error");
						throw new InvalidOperationException("Operation failed: " + errorMessage);
					}

					return status.Value<RavenJToken>("State");
				}
					

				await Task.Delay(500).ConfigureAwait(false);
			}
		}

		public RavenJToken WaitForCompletion()
		{
			return WaitForCompletionAsync().ResultUnwrap();
		}
	}
}
