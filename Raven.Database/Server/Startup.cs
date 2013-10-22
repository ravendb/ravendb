using Owin;
using Raven.Database.Config;

namespace Raven.Database.Server
{
	internal class Startup
	{
		private readonly RavenDBOptions options;

		public Startup(InMemoryRavenConfiguration config)
		{
			options = new RavenDBOptions(config);
		}

		public RavenDBOptions Options
		{
			get { return options; }
		}

		public void Configuration(IAppBuilder app)
		{
			app.UseRavenDB(options);
		}
	}
}