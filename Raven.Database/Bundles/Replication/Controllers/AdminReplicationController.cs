using System;
using System.ComponentModel.Composition;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Controllers.Admin;

namespace Raven.Database.Bundles.Replication.Controllers
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(RavenApiController))]
	public class AdminReplicationController : BaseAdminController
	{
		[HttpGet("admin/replication/purge-tombstones")]
		public HttpResponseMessage PurgeTombstones()
		{
			var docEtagStr = GetQueryStringValue("docEtag");
			Etag docEtag = null;
			var attachmentEtagStr = GetQueryStringValue("attachmentEtag");
			Etag attachmentEtag = null;
			try
			{
				docEtag = Etag.Parse(docEtagStr);
			}
			catch
			{
				try
				{
					attachmentEtag = Etag.Parse(attachmentEtagStr);
				}
				catch (Exception)
				{
					return GetMessageWithObject(new
					{
						Error = "The query string variable 'docEtag' or 'attachmentEtag' must be set to a valid guid"
					}, HttpStatusCode.BadRequest);
				}
			}

			Database.TransactionalStorage.Batch(accessor =>
			{
				if (docEtag != null)
				{
					accessor.Lists.RemoveAllBefore(Constants.RavenReplicationDocsTombstones, docEtag);
				}
				if (attachmentEtag != null)
				{
					accessor.Lists.RemoveAllBefore(Constants.RavenReplicationAttachmentsTombstones, attachmentEtag);
				}
			});

			return GetEmptyMessage();
		}
	}
}
