using System.ComponentModel.Composition;
using Raven.Database.Config;

namespace Raven.Database.Plugins
{
	[InheritedExport]
	public interface IAlterConfiguration
	{
		void AlterConfiguration(InMemoryRavenConfiguration configuration);
	}
}