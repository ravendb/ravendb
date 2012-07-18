// -----------------------------------------------------------------------
//  <copyright file="Notifications.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Imports.SignalR.Hubs;

namespace Raven.Database.Server
{
	public class NotificationsHub : Hub
	{
		public Task StartWatchingIndex(string indexName)
		{
			return Groups.Add(Context.ConnectionId, "indexes/" + indexName);
		}

		public Task StopWatchingIndex(string indexName)
		{
			return Groups.Remove(Context.ConnectionId, "indexes/" + indexName);
		}

		public Task StartWatchingDocument(string docId)
		{
			return Groups.Add(Context.ConnectionId, "docs/" + docId);
		}

		public Task StopWatchingDocument(string docId)
		{
			return Groups.Remove(Context.ConnectionId, "docs/" + docId);
		}
	}
}