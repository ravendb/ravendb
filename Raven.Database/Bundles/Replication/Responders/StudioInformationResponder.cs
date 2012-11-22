// -----------------------------------------------------------------------
//  <copyright file="StudioInformationResponder.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
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
	public class StudioInformationResponder : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/replication/info$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			Guid mostRecentDocumentEtag = Guid.Empty;
			Guid mostRecentAttachmentEtag = Guid.Empty;
			var replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
			Database.TransactionalStorage.Batch(accessor =>
			{
				mostRecentDocumentEtag = accessor.Staleness.GetMostRecentDocumentEtag();
				mostRecentAttachmentEtag = accessor.Staleness.GetMostRecentAttachmentEtag();
			});

			context.WriteJson(RavenJObject.FromObject(new ReplicationStatistics
			{
				Self = Database.ServerUrl,
				MostRecentDocumentEtag = mostRecentDocumentEtag,
				MostRecentAttachmentEtag = mostRecentAttachmentEtag,
				Stats = replicationTask == null
					        ? new List<DestinationStats>()
					        : replicationTask.DestinationStats.Values.ToList()
			}));
		}
	}
}