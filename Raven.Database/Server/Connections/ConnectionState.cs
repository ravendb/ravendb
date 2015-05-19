namespace Raven.Database.Server.Connections
{
	public class ConnectionState
	{
		private IEventsTransport eventsTransport;

		public ConnectionState(IEventsTransport eventsTransport)
		{
			this.eventsTransport = eventsTransport;
			DocumentStore = new DocumentsConnectionState(Enqueue);
			FileSystem = new FileSystemConnectionState(Enqueue);
			CounterStorage = new CounterStorageConnectionState(Enqueue);
		}

		public DocumentsConnectionState DocumentStore { get; private set; }
		public FileSystemConnectionState FileSystem { get; private set; }
		public CounterStorageConnectionState CounterStorage { get; private set; }

		public object DebugStatus
		{
			get
			{
				return new
				{
					eventsTransport.Id,
					eventsTransport.Connected,
					DocumentStore = DocumentStore.DebugStatus,
					FileSystem = FileSystem.DebugStatus,
					CounterStorage = CounterStorage.DebugStatus
				};
			}
		}

		private void Enqueue(object msg)
		{
			if (eventsTransport == null || eventsTransport.Connected == false)
			{
				return;
			}

			eventsTransport.SendAsync(msg);
		}

		public void Reconnect(IEventsTransport transport)
		{
			eventsTransport = transport;
		}

		public void Dispose()
		{
			if (eventsTransport != null)
				eventsTransport.Dispose();
		}
	}
}