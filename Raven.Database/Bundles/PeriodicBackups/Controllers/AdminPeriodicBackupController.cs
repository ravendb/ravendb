// -----------------------------------------------------------------------
//  <copyright file="AdminPeriodicBackupController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Server.Controllers;

namespace Raven.Database.Bundles.PeriodicBackups.Controllers
{
    public class AdminPeriodicBackupController : AdminBundlesApiController
    {
        public override string BundleName
        {
            get { return "PeriodicBackup"; }
        }

        [HttpPost]
        [Route("admin/periodicBackup/purge-tombstones")]
        public HttpResponseMessage PurgeTombstones()
        {
            var docEtagStr = GetQueryStringValue("docEtag");
            Etag docEtag = null;
            var attachmentEtagStr = GetQueryStringValue("attachmentEtag");
            Etag attachmentEtag = null;

            if (string.IsNullOrEmpty(docEtagStr) == false)
            {
                docEtag = Etag.Parse(docEtagStr);
            }

            if (string.IsNullOrEmpty(attachmentEtagStr) == false)
            {
                attachmentEtag = Etag.Parse(attachmentEtagStr);
            }

            if (docEtag == null && attachmentEtag == null)
            {
                return GetMessageWithObject(new
                {
                    Error = "The query string variable 'docEtag' or 'attachmentEtag' must be set to a valid etag"
                }, HttpStatusCode.BadRequest);
            }

            Database.TransactionalStorage.Batch(accessor =>
            {
                if (docEtag != null)
                {
                    accessor.Lists.RemoveAllBefore(Constants.RavenPeriodicBackupsDocsTombstones, docEtag);
                }
                if (attachmentEtag != null)
                {
                    accessor.Lists.RemoveAllBefore(Constants.RavenPeriodicBackupsAttachmentsTombstones, attachmentEtag);
                }
            });

            return GetEmptyMessage();
        }
    }
}