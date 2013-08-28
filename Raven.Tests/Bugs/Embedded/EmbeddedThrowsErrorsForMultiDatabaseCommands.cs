using System;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Xunit;

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
				var exception = Assert.Throws<NotSupportedException>(() => embeddableDocumentStore.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("test"));
				Assert.Equal(exception.Message, "Multiple databases are not supported in the embedded API currently");
			}
		}
	}
}