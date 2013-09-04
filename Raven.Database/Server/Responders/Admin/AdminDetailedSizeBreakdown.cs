// -----------------------------------------------------------------------
//  <copyright file="AdminSize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Server.Abstractions;
using Raven.Database.Storage.Esent.Debug;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders.Admin
{
    public class AdminDetailedSizeBreakdown : AdminResponder
    {

        public override string UrlPattern
        {
            get { return "^/admin/detailed-storage-breakdown?$"; }
        }
        public override string[] SupportedVerbs
        {
            get { return new[] {"GET"}; }
        }

        public override void RespondToAdmin(IHttpContext context)
        {
            var x = Database.TransactionalStorage.ComputeDetailedStorageInformation();
            context.WriteJson(x);
        }
    }
}