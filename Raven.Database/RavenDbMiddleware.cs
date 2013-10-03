using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Web.Http;
using Microsoft.Owin;
using Microsoft.Owin.Builder;
using Owin;
using Raven.Database.Server.WebApi;

namespace Raven.Database
{
    internal class RavenDbMiddleware : OwinMiddleware
    {
        private readonly Func<IDictionary<string, object>, Task> branch;

        public RavenDbMiddleware(OwinMiddleware next, RavenDbOwinOptions options)
            : base(next)
        {
            var httpConfiguration = new HttpConfiguration();
            WebApiServer.SetupConfig(httpConfiguration, options.Landlord, options.MixedModeRequestAuthorizer);

            options.Branch.UseWebApi(httpConfiguration);
            branch = options.Branch.Build();
        }

        public override Task Invoke(IOwinContext context)
        {
            return branch.Invoke(context.Environment);
        }
    }
}