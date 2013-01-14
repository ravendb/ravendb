using System;
using Raven.Client.Embedded;
using Xunit;
using Raven.Client.Extensions;

namespace Raven.Tests.Bugs.Embedded
{
	public class EmbeddedThrowsErrorsForMultiDatabaseCommands
	{
		[Fact]
		public void WontLetCreateADatabase()
		{
			using (var embeddableDocumentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize())
			{
				var exception = Assert.Throws<InvalidOperationException>(() => embeddableDocumentStore.DatabaseCommands.EnsureDatabaseExists("test"));
				Assert.Equal(exception.Message, "Multiple databases are not supported in the embedded API currently");
			}
		}
	}
}
