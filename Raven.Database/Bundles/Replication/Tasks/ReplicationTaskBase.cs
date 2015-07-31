// -----------------------------------------------------------------------
//  <copyright file="ItemsReplicationTaskBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Tasks
{
	public abstract class ReplicationTaskBase : IDisposable
	{
		protected readonly object emptyRequestBody = new object();
		protected readonly DocumentDatabase database;
		protected readonly HttpRavenRequestFactory httpRavenRequestFactory;

		protected ReplicationTaskBase(DocumentDatabase database, HttpRavenRequestFactory httpRavenRequestFactory)
		{
			this.database = database;
			this.httpRavenRequestFactory = httpRavenRequestFactory;
		}

		protected string GetDebugInformation()
		{
			return Constants.IsReplicatedUrlParamName + "=true&from=" + Uri.EscapeDataString(database.ServerUrl);
		}

		protected List<JsonDocument> GetTombstones(string tombstoneListName, int start, int take, Func<ListItem, bool> wherePredicate = null)
		{
			List<JsonDocument> tombstones = null;

			database.TransactionalStorage.Batch(actions =>
			{
				var getTombstones = actions
					.Lists
					.Read(tombstoneListName, start, take);

				if (wherePredicate != null)
				{
					getTombstones = getTombstones.Where(wherePredicate);
				}

				tombstones = getTombstones.Select(x => new JsonDocument
				{
					Etag = x.Etag,
					Key = x.Key,
					Metadata = x.Data,
					DataAsJson = new RavenJObject()
				}).ToList();
			});

			return tombstones ?? new List<JsonDocument>();
		}

		public abstract void Dispose();
	}
}