using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_187 : RavenTest
	{
		[Fact]
		public void ShouldWork()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("users/1", null, new RavenJObject(), new RavenJObject
				{
					{Constants.RavenDeleteMarker, "true"}
				});

				using (var s = store.OpenSession())
				{
					s.Advanced.UseOptimisticConcurrency = true;
					s.Store(new User());
					s.SaveChanges();
				}
			}
		}
	}
}