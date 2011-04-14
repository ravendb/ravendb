//-----------------------------------------------------------------------
// <copyright file="ReplicationLastEtagResponser.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Database.Server.Responders;
using Raven.Database.Json;
using log4net;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Bundles.Replication.Reponsders
{
    public class ReplicationLastEtagResponser : RequestResponder
    {
        private ILog log = LogManager.GetLogger(typeof (ReplicationLastEtagResponser));

        public override void Respond(IHttpContext context)
        {
            var src = context.Request.QueryString["from"];
            if (string.IsNullOrEmpty(src))
            {
                context.SetStatusToBadRequest();
                return;
            }
            while (src.EndsWith("/"))
                src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven
            if (string.IsNullOrEmpty(src))
            {
                context.SetStatusToBadRequest();
                return;
            }
			using (Database.DisableAllTriggersForCurrentThread())
            {
                var document = Database.Get(ReplicationConstants.RavenReplicationSourcesBasePath + "/" + src, null);
                if (document == null)
                {
                    log.DebugFormat("Got replication last etag request from {0}: [{1}]", src, new SourceReplicationInformation());
                    context.WriteJson(new SourceReplicationInformation());
                }
                else
                {
                    var sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
                    log.DebugFormat("Got replication last etag request from {0}: [{1}]", src, sourceReplicationInformation);
                    context.WriteJson(sourceReplicationInformation);
                }
            }
        }

        public override string UrlPattern
        {
            get { return "^/replication/lastEtag$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET" }; }
        }
    }
}
