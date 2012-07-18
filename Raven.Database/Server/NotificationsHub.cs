// -----------------------------------------------------------------------
//  <copyright file="Notifications.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Imports.SignalR.Hubs;

namespace Raven.Database.Server
{
	public class NotificationsHub : Hub
	{
		public void StartWatchingIndex(string indexName)
		{
			Groups.Add(Context.ConnectionId, "indexes/" + indexName);
		}

		public void StopWatchingIndex(string indexName)
		{
			Groups.Remove(Context.ConnectionId, "indexes/" + indexName);
		}

		public void StartWatchingDocument(string docId)
		{
			Groups.Add(Context.ConnectionId, "docs/" + docId);
		}

		public void StopWatchingDocument(string docId)
		{
			Groups.Remove(Context.ConnectionId, "docs/" + docId);
		}
	}
}