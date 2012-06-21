using System;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Database.Plugins.Builtins;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class NoBuiltinDuplicates
	{
		[Fact]
		public void ShouldNotHaveDuplicates()
		{
			var compositionContainer = new InMemoryRavenConfiguration
			{
				PluginsDirectory = AppDomain.CurrentDomain.BaseDirectory
			}.Container;

			var enumerable = compositionContainer.GetExportedValues<AbstractReadTrigger>().ToList();
			Assert.Equal(1, enumerable.Count(x => x is FilterRavenInternalDocumentsReadTrigger));
		}
	}
}