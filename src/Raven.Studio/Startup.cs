using Microsoft.AspNet.Builder;
using Microsoft.AspNet.Hosting;

namespace Raven.Studio
{
    public class Startup
    {
        public void Configure(IApplicationBuilder app)
        {
            app.UseStaticFiles();
        }

        public static void Main(string[] args) => WebApplication.Run<Startup>(args);
    }
}
