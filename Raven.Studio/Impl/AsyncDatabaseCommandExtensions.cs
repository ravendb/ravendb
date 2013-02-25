using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Client.Connection;

namespace Raven.Studio.Impl
{
	public static class AsyncDatabaseCommandExtensions
	{
		public static Task EnsureSilverlightStartUpAsync(this IAsyncDatabaseCommands self)
		{
			return self.CreateRequest("/silverlight/ensureStartup".NoCache(), "GET")
				.ExecuteRequestAsync();
		}		 
	}
}