// -----------------------------------------------------------------------
//  <copyright file="DebugDocumentsCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders.Debugging
{
    public class DebugDocumentsCount : AbstractRequestResponder
    {
        public override string UrlPattern
        {
            get { return @"^/debug/sl0w-d0c-c0unts"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET" }; }
        }

        public override void Respond(IHttpContext context)
        {
            DebugDocumentStats stat = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                stat = accessor.Documents.GetDocumentStatsVerySlowly();
            });

            context.WriteJson(stat);
        }

       
    }
}