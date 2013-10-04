using System;
using System.Threading;
using System.Web.Http;
using Raven.Database;
using Raven.Database.Server.WebApi;

// ReSharper disable once CheckNamespace
namespace Owin
{
    public static class AppBuilderExtensions
    {
        // TODO Ext method where InMemoryRavenConfiguration and DocumentDatabase are defaults
        public static IAppBuilder UseRavenDB(this IAppBuilder app, RavenDBOptions options)
        {
            if (options == null)
            {
                throw new ArgumentNullException("options");
            }

            DocumentDatabase documentDatabase = options.DocumentDatabase;
            
            var appDisposing = app.Properties["host.OnAppDisposing"] as CancellationToken?;
            if (appDisposing.HasValue)
            {
                appDisposing.Value.Register(documentDatabase.Dispose);
            }

            var httpConfiguration = new HttpConfiguration();
            WebApiServer.SetupConfig(httpConfiguration, options.Landlord, options.MixedModeRequestAuthorizer);
            app.UseWebApi(httpConfiguration);
            return app;
        }
    }
}