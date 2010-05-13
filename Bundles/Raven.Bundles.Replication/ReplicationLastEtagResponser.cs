using System;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using Raven.Database.Json;

namespace Raven.Bundles.Replication
{
    public class ReplicationLastEtagResponser : RequestResponder
    {
        public override void Respond(IHttpContext context)
        {
            var src = context.Request.QueryString["from"];
            if(string.IsNullOrEmpty(src))
            {
                context.SetStatusToBadRequest();
                return;
            }
            var document = Database.Get(ReplicationConstants.RavenReplicationSourcesBasePath + "/" + src, null);
            if(document == null)
            {
                context.WriteJson(new{Etag = Guid.Empty});
            }
            else
            {
                var sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
                context.WriteJson(new { Etag = sourceReplicationInformation.LastEtag });
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