using Microsoft.Owin;
using Owin;

[assembly: OwinStartup(typeof(Raven.Tests.Web.Startup))]
namespace Raven.Tests.Web
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
        }
    }
}
