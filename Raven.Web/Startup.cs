using Owin;

namespace Raven.Web
{
    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            app.UseRavenDB();
        }
    }
}