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
	public class Notifications : PersistentConnection
	{
		public event EventHandler Disposed = delegate { }; 
		
		private HttpServer httpServer;
		private string theConnectionId;

		public void Send(ChangeNotification notification)
		{
			Connection.Send(theConnectionId, notification);
		}
		public override void Initialize(IDependencyResolver resolver)
		{
			httpServer = resolver.Resolve<HttpServer>();
			base.Initialize(resolver);
		}

		protected override System.Threading.Tasks.Task OnConnectedAsync(IRequest request, string connectionId)
		{
			this.theConnectionId = connectionId;
			var db = request.QueryString["database"];
			if(string.IsNullOrEmpty(db))
				throw new ArgumentException("The database query string element is mandatory");

			httpServer.RegisterConnection(db, this);

			return base.OnConnectedAsync(request, connectionId);
		}

		protected override System.Threading.Tasks.Task OnDisconnectAsync(string connectionId)
		{
			Disposed(this, EventArgs.Empty);
			return base.OnDisconnectAsync(connectionId);
		}
	}
}