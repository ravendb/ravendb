using System;
using System.Web.Http;
using Raven.Database;
using Raven.Database.Server.WebApi;

// ReSharper disable once CheckNamespace
namespace Owin
{
    public static class AppBuilderExtensions
    {
        // TODO Ext method where InMemoryRavenConfiguration and DocumentDatabase are defaults
        public static IAppBuilder UseRavenDB(this IAppBuilder app, RavenDBOwinOptions options)
        {
            if (options == null)
            {
                throw new ArgumentNullException("options");
            }
            var httpConfiguration = new HttpConfiguration();
            WebApiServer.SetupConfig(httpConfiguration, options.Landlord, options.MixedModeRequestAuthorizer);
            app.UseWebApi(httpConfiguration);
            return app;
        }
    }
}