using Owin;
using Raven.Database;
using Raven.Database.Config;

namespace Raven.Web
{
    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            app.UseRavenDB(new RavenDBOptions(new RavenConfiguration()));
        }
    }
}