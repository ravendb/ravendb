using System;
using System.Threading;
using System.Web.Http;
using Raven.Database;
using Raven.Database.Server.WebApi;

// ReSharper disable once CheckNamespace
namespace Owin
{
    using Raven.Database.Config;

    public static class AppBuilderExtensions
    {
        public static IAppBuilder UseRavenDB(this IAppBuilder app)
        {
            return UseRavenDB(app, new InMemoryRavenConfiguration());
        }

        public static IAppBuilder UseRavenDB(this IAppBuilder app, InMemoryRavenConfiguration configuration)
        {
            return UseRavenDB(app, new RavenDBOptions(configuration));
        }

        public static IAppBuilder UseRavenDB(this IAppBuilder app, RavenDBOptions options)
        {
            if (options == null)
            {
                throw new ArgumentNullException("options");
            }

            DocumentDatabase documentDatabase = options.DocumentDatabase;
            
            // This is a katana specific key (i.e. non a standard OWIN key) to be notified
            // when the host in being shut down. Works both in HttpListener and SystemWeb hosting
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