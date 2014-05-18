using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class ListCount : RavenTest
	{
		public class Location
		{
			public Guid Id { get; set; }

			public string Name { get; set; }

			public List<Guid> Properties { get; set; }
		}

		[Fact]
		public void CanGetCount()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Location
					{
						Properties = new List<Guid>
						{
							Guid.NewGuid(),
							Guid.NewGuid()
						},
						Name = "Ayende"
					});
					session.SaveChanges();

					var result = session.Query<Location>()
						.Where(x => x.Name.StartsWith("ay"))
						.Select(x => new
						{
							x.Name,
							x.Properties.Count
						}).ToList();

					Assert.Equal("Ayende", result[0].Name);
					Assert.Equal(2, result[0].Count);
				}
			}
		}
	}
}