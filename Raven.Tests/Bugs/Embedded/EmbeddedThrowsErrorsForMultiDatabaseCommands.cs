using System;
using Raven.Client.Extensions;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Embedded
{
	public class EmbeddedThrowsErrorsForMultiDatabaseCommands : RavenTest
	{
		[Fact]
		public void WontLetCreateADatabase()
		{
			using (var store = NewDocumentStore())
			{
				var exception = Assert.Throws<NotSupportedException>(() => store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("test"));
				Assert.Equal(exception.Message, "Multiple databases are not supported in the embedded API currently");
			}
		}
	}
}