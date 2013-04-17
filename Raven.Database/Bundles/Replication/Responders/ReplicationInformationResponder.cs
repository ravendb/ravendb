// -----------------------------------------------------------------------
//  <copyright file="ReplicationInformationResponder.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Bundles.Replication;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;
using System.Linq;
using Raven.Json.Linq;
using Raven.Abstractions.Replication;

namespace Raven.Bundles.Replication.Responders
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof (AbstractRequestResponder))]
	public class ReplicationInformationResponder : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/replication/info$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET","POST"}; }
		}

		public override void Respond(IHttpContext context)
		{
			Etag mostRecentDocumentEtag = Etag.Empty;
			Etag mostRecentAttachmentEtag = Etag.Empty;
			Database.TransactionalStorage.Batch(accessor =>
			{
				mostRecentDocumentEtag = accessor.Staleness.GetMostRecentDocumentEtag();
				mostRecentAttachmentEtag = accessor.Staleness.GetMostRecentAttachmentEtag();
			});

			var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			var replicationStatistics = new ReplicationStatistics
			{
				Self = Database.ServerUrl,
				MostRecentDocumentEtag = mostRecentDocumentEtag,
				MostRecentAttachmentEtag = mostRecentAttachmentEtag,
				Stats = replicationTask == null ? new List<DestinationStats>() : replicationTask.DestinationStats.Values.ToList()
			};
			context.WriteJson(RavenJObject.FromObject(replicationStatistics));
		}
	}
}