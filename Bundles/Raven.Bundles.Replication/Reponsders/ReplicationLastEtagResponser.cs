using System;
using Raven.Bundles.Replication.Data;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using Raven.Database.Json;
using log4net;

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
			using (ReplicationContext.Enter())
            {
                var document = Database.Get(ReplicationConstants.RavenReplicationSourcesBasePath + "/" + src, null);
                if (document == null)
                {
                    log.DebugFormat("Got replication last etag request from {0}: [{1}]", src, Guid.Empty);
                    context.WriteJson(new { Etag = Guid.Empty });
                }
                else
                {
                    var sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
                    log.DebugFormat("Got replication last etag request from {0}: [{1}]", src, sourceReplicationInformation.LastEtag);
                    context.WriteJson(new { Etag = sourceReplicationInformation.LastEtag });
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
