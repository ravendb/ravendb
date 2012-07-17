using System.Threading.Tasks;
using Raven.Imports.SignalR.Client.Hubs;

namespace Raven.Client.Changes
{
	public class DatabaseChanges
	{
		private readonly HubConnection hubConnection;

		public DatabaseChanges(string url)
		{
			hubConnection = new HubConnection(url);
			Task = hubConnection.Start();
		}

		public Task Task { get; private set; }

		public DatabaseChanges IndexSubscription(string indexName)
		{
			
		}

		public DatabaseChanges DocumentSubscription(string documentSubscription)
		{
			
		}
	}
}