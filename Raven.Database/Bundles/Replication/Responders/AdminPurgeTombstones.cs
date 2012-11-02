using System;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders.Admin;
using Raven.Database.Extensions;

namespace Raven.Bundles.Replication.Responders
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(AbstractRequestResponder))]
	public class AdminPurgeTombstones : AdminResponder
	{
		public override string UrlPattern
		{
			get { return "^/admin/replication/purge-tombstones$"; }
	
		}
		public override void RespondToAdmin(IHttpContext context)
		{
			var etagStr = context.Request.QueryString["etag"];
			Guid etag;
			if(Guid.TryParse(etagStr, out etag) == false)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "The query string variable 'etag' must be set to a valid guid"
				});
				return;
			}

			Database.TransactionalStorage.Batch(accessor =>
			{
				accessor.Lists.RemoveAllBefore(Constants.RavenReplicationDocsTombstones, etag);
				accessor.Lists.RemoveAllBefore(Constants.RavenReplicationAttachmentsTombstones, etag);
			});
		}
	}
}