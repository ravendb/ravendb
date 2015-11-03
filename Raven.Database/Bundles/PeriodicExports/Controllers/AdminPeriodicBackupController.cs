// -----------------------------------------------------------------------
//  <copyright file="AdminPeriodicExportController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;
using System.Net.Http;
using System.Web.Http;

using Raven.Abstractions.Data;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;

namespace Raven.Database.Bundles.PeriodicExports.Controllers
{
    public class AdminPeriodicExportController : AdminBundlesApiController
    {
        public override string BundleName
        {
            get { return "PeriodicExport"; }
        }

        [HttpPost]
        [RavenRoute("admin/periodicExport/purge-tombstones")]
        [RavenRoute("databases/{databaseName}/admin/periodicExport/purge-tombstones")]
        public HttpResponseMessage PurgeTombstones()
        {
            var docEtagStr = GetQueryStringValue("docEtag");
            var attachmentEtagStr = GetQueryStringValue("attachmentEtag");

            Etag docEtag, attachmentEtag;

            var docEtagParsed = Etag.TryParse(docEtagStr, out docEtag);
            var attachmentEtagParsed = Etag.TryParse(attachmentEtagStr, out attachmentEtag);

            if (docEtagParsed == false && attachmentEtagParsed == false)
            {
                return GetMessageWithObject(
                    new
                    {
                        Error = "The query string variable 'docEtag' or 'attachmentEtag' must be set to a valid etag"
                    }, HttpStatusCode.BadRequest);
            }

            Database.TransactionalStorage.Batch(accessor =>
            {
                if (docEtag != null)
                {
                    accessor.Lists.RemoveAllBefore(Constants.RavenPeriodicExportsDocsTombstones, docEtag);
                }
                if (attachmentEtag != null)
                {
                    accessor.Lists.RemoveAllBefore(Constants.RavenPeriodicExportsAttachmentsTombstones, attachmentEtag);
                }
            });

            return GetEmptyMessage();
        }
    }
}
