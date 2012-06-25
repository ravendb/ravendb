using System;
using System.ComponentModel.Composition.Hosting;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Database.Plugins.Builtins;
using Raven.Database.Plugins.Catalogs;
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
				// can't use that, we have some unloadable assemblies in the base directory, 
				// instead, testing the filtering catalog itself
				Catalog =
					{
						Catalogs =
							{
								new BuiltinFilteringCatalog(new AssemblyCatalog(typeof(DocumentDatabase).Assembly))
							}
					}
			}.Container;

			var enumerable = compositionContainer.GetExportedValues<AbstractReadTrigger>().ToList();
			Assert.Equal(1, enumerable.Count(x => x is FilterRavenInternalDocumentsReadTrigger));
		}
	}
}