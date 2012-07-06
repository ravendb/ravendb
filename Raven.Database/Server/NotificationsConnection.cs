// -----------------------------------------------------------------------
//  <copyright file="Notifications.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using Raven.Abstractions.Data;
using Raven.Imports.SignalR;

namespace Raven.Database.Server
{
	public class NotificationsConnection : PersistentConnection
	{
		public event EventHandler Disposed = delegate { }; 
		
		private HttpServer httpServer;
		private string theConnectionId;
		private DocumentDatabase db;
		private string idPrefix;
		ChangeTypes changes;

		public void Send(ChangeNotification notification)
		{
			if ((notification.Type & changes) == ChangeTypes.None)
				return;

			if (string.IsNullOrEmpty(idPrefix) == false &&
				notification.Name.StartsWith(idPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
				return;

			Connection.Send(theConnectionId, notification);
		}
		public override void Initialize(IDependencyResolver resolver)
		{
			httpServer = resolver.Resolve<HttpServer>();
			db = httpServer.CurrentDatabase;
			base.Initialize(resolver);
		}

		protected override System.Threading.Tasks.Task OnConnectedAsync(IRequest request, string connectionId)
		{
			this.theConnectionId = connectionId;

			httpServer.RegisterConnection(db, this);

			var changesAsStr = request.QueryString["changes"];

			if(Enum.TryParse(changesAsStr, out changes) == false)
				changes = ChangeTypes.Common;

			idPrefix = request.QueryString["idPrefix"];

			return base.OnConnectedAsync(request, connectionId);
		}

		protected override System.Threading.Tasks.Task OnDisconnectAsync(string connectionId)
		{
			Disposed(this, EventArgs.Empty);
			return base.OnDisconnectAsync(connectionId);
		}
	}
}