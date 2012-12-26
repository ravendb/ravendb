using System;
using Raven.Client.Embedded;
using Xunit;
using Raven.Client.Extensions;

namespace Raven.Tests.Bugs.Embedded
{
	public class EmdeddedThrowsErrorsForMultiDatabaseCommands
	{
		[Fact]
		public void WontLetCreateADatabase()
		{
			using (var embeddableDocumentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize())
			{
				var exeption = Assert.Throws<InvalidOperationException>(() => embeddableDocumentStore.DatabaseCommands.EnsureDatabaseExists("test"));
				Assert.Equal(exeption.Message, "Multiple databases are not supported in the embedded API");
			}
		}
	}
}
