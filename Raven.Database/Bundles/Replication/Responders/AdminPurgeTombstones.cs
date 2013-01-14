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
			var docEtagStr = context.Request.QueryString["docEtag"];
			Guid docEtag;
			var attachmentEtagStr = context.Request.QueryString["attachmentEtag"];
			Guid attachmentEtag;
			if (Guid.TryParse(docEtagStr, out docEtag) == false & // intentionally so, we want to eval both sides
				Guid.TryParse(attachmentEtagStr, out attachmentEtag) == false)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "The query string variable 'docEtag' or 'attachmentEtag' must be set to a valid guid"
				});
				return;
			}

			Database.TransactionalStorage.Batch(accessor =>
			{
				if(docEtag != Guid.Empty)
				{
					accessor.Lists.RemoveAllBefore(Constants.RavenReplicationDocsTombstones, docEtag);
				}
				if(attachmentEtag != Guid.Empty)
				{
					accessor.Lists.RemoveAllBefore(Constants.RavenReplicationAttachmentsTombstones, attachmentEtag);
				}
			});
		}
	}
}