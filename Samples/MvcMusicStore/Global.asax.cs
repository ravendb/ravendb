using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Routing;
using Raven.Client;
using Raven.Client.Document;

namespace MvcMusicStore
{
    // Note: For instructions on enabling IIS6 or IIS7 classic mode, 
    // visit http://go.microsoft.com/?LinkId=9394801

    public class MvcApplication : System.Web.HttpApplication
    {
        private const string RavenSessionKey = "Raven.Session";
        private static DocumentStore _documentStore;

        protected void Application_Start()
        {
            _documentStore = new DocumentStore { Url = "http://localhost:8080/" };
            _documentStore.Initialise();

            AreaRegistration.RegisterAllAreas();

            RegisterRoutes(RouteTable.Routes);
        }

        public MvcApplication()
        {
            BeginRequest += (sender, args) => HttpContext.Current.Items[RavenSessionKey] = _documentStore.OpenSession();
            EndRequest += (o, eventArgs) =>
            {
                var disposable = HttpContext.Current.Items[RavenSessionKey] as IDisposable;
                if (disposable != null)
                    disposable.Dispose();
            };
        }

        public static IDocumentSession CurrentSession
        {
            get { return (IDocumentSession) HttpContext.Current.Items[RavenSessionKey]; }
        }

        public static void RegisterRoutes(RouteCollection routes)
        {
            routes.IgnoreRoute("{resource}.axd/{*pathInfo}");

            // urls with raven's id, which include /
            routes.MapRoute(
                "WithParam",                                              // Route name
                "{controller}/{action}/{*id}"                         // URL with parameters
                );

            // paramter less urls
            routes.MapRoute(
              "Default",                                              // Route name
              "{controller}/{action}",                           // URL with parameters
              new { controller = "Home", action = "Index" }  // Parameter defaults
              );
          
        }
    }
}