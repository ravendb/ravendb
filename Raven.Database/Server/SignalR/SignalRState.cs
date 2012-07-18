// -----------------------------------------------------------------------
//  <copyright file="SignalRState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Util;
using Raven.Imports.SignalR;
using System.Linq;

namespace Raven.Database.Server.SignalR
{
	public class SignalRState
	{
		readonly TimeSensitiveStore<string> timeSensitiveStore = new TimeSensitiveStore<string>(TimeSpan.FromSeconds(45));

		readonly ConcurrentDictionary<string, ConnectionState> connections = new ConcurrentDictionary<string, ConnectionState>();

		public TimeSensitiveStore<string> TimeSensitiveStore
		{
			get { return timeSensitiveStore; }
		}

		public void OnIdle()
		{
			ConnectionState _;
			timeSensitiveStore.ForAllExpired(s => connections.TryRemove(s, out _));
		}

		public ConnectionState Register(string connectionId, IConnection connection)
		{
			timeSensitiveStore.Seen(connectionId);
			return connections.GetOrAdd(connectionId, new ConnectionState(connectionId, connection));
		}

		public void Send(IndexChangeNotification indexChangeNotification)
		{
			foreach (var connectionState in connections)
			{
				connectionState.Value.Send(indexChangeNotification);
			}
		}

		public void Send(DocumentChangeNotification documentChangeNotification)
		{
			foreach (var connectionState in connections)
			{
				connectionState.Value.Send(documentChangeNotification);
			}
		}
	}

	public class ConnectionState
	{
		private readonly string id;
		private readonly IConnection connection;
		private readonly ConcurrentSet<string> matchingIndexes =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> matchingDocuments =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentSet<string> matchingDocumentPrefixes =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		
		
		private int watchAllDocuments;

		public ConnectionState(string id, IConnection connection)
		{
			this.id = id;
			this.connection = connection;
		}

		
		public void WatchIndex(string name)
		{
			matchingIndexes.TryAdd(name);
		}

		public void UnwatchIndex(string name)
		{
			matchingIndexes.TryRemove(name);
		}

		public void Send(DocumentChangeNotification documentChangeNotification)
		{
			var value = new { Value = documentChangeNotification, Type = "DocumentChangeNotification" };
			if (watchAllDocuments > 0)
			{
				connection.Send(id, value);
				return;
			}

			if(matchingDocuments.Contains(documentChangeNotification.Name))
			{
				connection.Send(id, value);
				return;
			}

			var hasPrefix = matchingDocumentPrefixes.Any(x => documentChangeNotification.Name.StartsWith(x, StringComparison.InvariantCultureIgnoreCase));
			if (hasPrefix == false)
				return;

			connection.Send(id, value);
		}

		public void Send(IndexChangeNotification indexChangeNotification)
		{
			if (matchingIndexes.Contains(indexChangeNotification.Name) == false)
				return;

			connection.Send(id, new { Value = indexChangeNotification, Type = "IndexChangeNotification" });
		}

		public void WatchAllDocuments()
		{
			Interlocked.Increment(ref watchAllDocuments);
		}

		public void UnwatchAllDocuments()
		{
			Interlocked.Decrement(ref watchAllDocuments);
		}

		public void WatchDocument(string name)
		{
			matchingDocuments.TryAdd(name);
		}

		public void UnwatchDocument(string name)
		{
			matchingDocuments.TryRemove(name);
		}

		public void WatchDocumentPrefix(string name)
		{
			matchingDocumentPrefixes.TryAdd(name);
		}

		public void UnwatchDocumentPrefix(string name)
		{
			matchingDocumentPrefixes.TryRemove(name);
		}
	}
}