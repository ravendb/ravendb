using System;
using System.Text.RegularExpressions;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Json;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using Raven.Http;

namespace Raven.Database.Server
{
    public class RavenDbHttpServer : HttpServer
    {
        private static readonly Regex databaseQuery = new Regex("^/databases/([^/]+)(?=/?)");

        public override Regex TenantsQuery
        {
            get { return databaseQuery; }
        }

        public RavenDbHttpServer(IRaveHttpnConfiguration configuration, IResourceStore database)
            : base(configuration, database)
        {
        }

        protected override bool TryHandleException(IHttpContext ctx, Exception e)
        {
            if (e is IndexDisabledException)
            {
                HandleIndexDisabledException(ctx, (IndexDisabledException)e);
                return true;
            }
            if (e is IndexDoesNotExistsException)
            {
                HandleIndexDoesNotExistsException(ctx, e);
                return true;
            }

            return false;
        }

        private static void HandleIndexDoesNotExistsException(IHttpContext ctx, Exception e)
        {
            ctx.SetStatusToNotFound();
            SerializeError(ctx, new
            {
                Url = ctx.Request.RawUrl,
                Error = e.Message
            });
        }

        private DocumentDatabase DefaultDatabase
        {
            get { return (DocumentDatabase) DefaultResourceStore; }
        }

        protected override bool TryGetOrCreateResourceStore(string tenantId, out IResourceStore database)
        {
            if (ResourcesStoresCache.TryGetValue(tenantId, out database))
                return true;

            var jsonDocument = DefaultDatabase.Get("Raven/Databases/" + tenantId, null);

            if (jsonDocument == null)
                return false;

            var document = jsonDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();

            database = ResourcesStoresCache.GetOrAdd(tenantId, s =>
            {
                var config = new InMemroyRavenConfiguration
                {
                    Settings = DefaultConfiguration.Settings,
                };
                config.Settings["Raven/VirtualDir"] = config.Settings["Raven/VirtualDir"] + "/" + tenantId;
                foreach (var setting in document.Settings)
                {
                    config.Settings[setting.Key] = setting.Value;
                }
                config.Initialize();
                return new DocumentDatabase(config);
            });
            return true;


        }


        private static void HandleIndexDisabledException(IHttpContext ctx, IndexDisabledException e)
        {
            ctx.Response.StatusCode = 503;
            ctx.Response.StatusDescription = "Service Unavailable";
            SerializeError(ctx, new
            {
                Url = ctx.Request.RawUrl,
                Error = e.Information.GetErrorMessage(),
                Index = e.Information.Name,
            });
        }
    }
}