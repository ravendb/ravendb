// -----------------------------------------------------------------------
//  <copyright file="ChangesConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Server.SignalR;
using Raven.Imports.SignalR;
using Raven.Json.Linq;

namespace Raven.Database.Server
{
	public class ChangesConnection : PersistentConnection
	{
		DocumentDatabase documentDatabase;
		SignalRState signalRState;
		public override void Initialize(IDependencyResolver resolver)
		{
			base.Initialize(resolver);
			var httpServer = resolver.Resolve<HttpServer>();
			documentDatabase = httpServer.CurrentDatabase;
			signalRState = documentDatabase.SignalRState;
		}

		protected override System.Threading.Tasks.Task OnConnectedAsync(IRequest request, string connectionId)
		{
			signalRState.Register(connectionId, Connection);
			return base.OnConnectedAsync(request, connectionId);
		}

		protected override System.Threading.Tasks.Task OnReceivedAsync(IRequest request, string connectionId, string data)
		{
			var jsonData = RavenJObject.Parse(data);
			
			var name = jsonData.Value<string>("Name");
			var connectionState = signalRState.Register(connectionId, Connection);
			switch (jsonData.Value<string>("Type"))
			{
				case "WatchIndex":
					connectionState.WatchIndex(name);
					break;
				case "UnwatchIndex":
					connectionState.UnwatchIndex(name);
					break;

				case "WatchDocument":
					connectionState.WatchDocument(name);
					break;
				case "UnwatchDocument":
					connectionState.UnwatchDocument(name);
					break;

				case "WatchAllDocuments":
					connectionState.WatchAllDocuments();
					break;
				case "UnwatchAllDocuments":
					connectionState.UnwatchAllDocuments();
					break;

				case "WatchDocumentPrefix":
					connectionState.WatchDocumentPrefix(name);
					break;
				case "UnwatchDocumentPrefix":
					connectionState.UnwatchDocumentPrefix(name);
					break;
			}

			return base.OnReceivedAsync(request, connectionId, data);
		}

		protected override System.Threading.Tasks.Task OnDisconnectAsync(string connectionId)
		{
			signalRState.TimeSensitiveStore.Missing(connectionId);
			return base.OnDisconnectAsync(connectionId);
		}

		protected override System.Threading.Tasks.Task OnReconnectedAsync(IRequest request, System.Collections.Generic.IEnumerable<string> groups, string connectionId)
		{
			signalRState.TimeSensitiveStore.Seen(connectionId);
			return base.OnReconnectedAsync(request, groups, connectionId);
		}
	}
}