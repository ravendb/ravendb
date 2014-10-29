using System;
using System.Collections.Generic;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class EqualityWithArrayOfGuids : RavenTest
	{
		public class Item
		{
			public List<Guid> Ids { get; set; }
		}

		[Fact]
		public void ShouldBeEqual()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new Item
					{
						Ids = new List<Guid>
						{
							Guid.NewGuid(),
							Guid.NewGuid(),
							Guid.NewGuid()
						}
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var item = s.Load<Item>("items/1");
					Assert.False(s.Advanced.HasChanged(item));
				}
			}
		}
	}
}