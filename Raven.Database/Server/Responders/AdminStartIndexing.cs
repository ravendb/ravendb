using System;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
    public class AdminStartIndexing : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "^/admin/startindexing$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "POST" }; }
        }

        public override void Respond(IHttpContext context)
        {
            if (context.User.Identity.IsAuthenticated == false ||
                context.User.IsInRole("Administrators"))
            {
                context.SetStatusToForbidden();
                context.WriteJson(new
                {
                    Error = "Only administrators can start indexing"
                });
                return;
            }

            var concurrency = context.Request.QueryString["concurrency"];

            if(string.IsNullOrEmpty(concurrency)==false)
            {
                Database.Configuration.MaxNumberOfParallelIndexTasks = Math.Max(1, int.Parse(concurrency));
            }
            
            Database.SpinBackgroundWorkers();
        }
    }
}