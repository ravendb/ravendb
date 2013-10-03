using System;
using Raven.Database;

// ReSharper disable once CheckNamespace
namespace Owin
{
    public static class AppBuilderExtensions
    {
        // TODO Ext method where InMemoryRavenConfiguration and DocumentDatabase are defaults
        public static IAppBuilder UseRavenDB(this IAppBuilder app, RavenDbOwinOptions options)
        {
            if (options == null)
            {
                throw new ArgumentNullException("options");
            }
            options.Branch = app.New();
            var result = app.Use<RavenDbMiddleware>(options);
            return result;
        }
    }
}