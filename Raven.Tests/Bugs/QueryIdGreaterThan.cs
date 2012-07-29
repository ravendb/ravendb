using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryIdGreaterThan : LocalClientTest
	{
		[Fact]
		public void CanQueryForIdGreaterThan()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new UserInt32 {
						Id = 1,
						Name = "Ayende"
					});
					s.Store(new UserInt32 {
						Id = 2,
						Name = "Itamar"
					});
					s.Store(new UserInt32 {
						Id = 3,
						Name = "Chris"
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var test1 = s.Query<UserInt32>().Customize(d => d.WaitForNonStaleResults()).Where(x => x.Id == 1 && x.Name == "Ayende").ToList();
					var test2 = s.Query<UserInt32>().Customize(d => d.WaitForNonStaleResults()).Where(x => x.Id >= 1).ToList();

					Assert.Equal(1, test1.Count);
					Assert.Equal(3, test2.Count);
				}
			}
		}
	}
}
