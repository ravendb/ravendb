using System;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
    public class AdminStopIndexing : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "^/admin/stopindexing$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"POST"}; }
        }

        public override void Respond(IHttpContext context)
        {
            if (context.IsAdministrator() == false)
            {
                context.SetStatusToForbidden();
                context.WriteJson(new
                {
                    Error = "Only administrators can stop indexing"
                });
                return;
            }


            Database.StopBackgroundWokers();
        }
    }
}