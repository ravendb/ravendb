using System;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public class Operation
	{
	    private readonly Func<long, Task<RavenJToken>> statusFetcher;
		private readonly long id;
		private readonly RavenJToken state;
		private readonly bool done;

		public Operation(long id, RavenJToken state)
		{
			this.id = id;
			this.state = state;
			this.done = true;
		}

        public Operation(Func<long, Task<RavenJToken>> statusFetcher, long id)
        {
            this.statusFetcher = statusFetcher;
            this.id = id;
        }

		public Operation(AsyncServerClient asyncServerClient, long id)
		    : this(asyncServerClient.GetOperationStatusAsync, id)
		{
		}


		public async Task<RavenJToken> WaitForCompletionAsync()
		{
			if (done)
				return state;
			if (statusFetcher == null)
				throw new InvalidOperationException("Cannot use WaitForCompletionAsync() when the operation was executed syncronously");

			while (true)
			{
                var status = await statusFetcher(id).ConfigureAwait(false);
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
