using Raven.Database.Config;
using Raven.Database.Storage.Esent;

namespace Raven.Database.Server.RavenFS.Storage.Esent
{
	public class TransactionalStorageConfigurator : StorageConfigurator
	{
		public TransactionalStorageConfigurator(InMemoryRavenConfiguration configuration)
			: base(configuration)
		{
		}

		protected override void ConfigureInstanceInternal(int maxVerPages)
		{
			// nothing to do here
		}

		protected override string BaseName
		{
			get
			{
				return "RFS";
			}
		}

		protected override string EventSource
		{
			get
			{
				return "RavenFS";
			}
		}
	}
}
