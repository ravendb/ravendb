using System;
using System.Linq;
using Raven.Client.Document;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class CustomEntityName : LocalClientTest
	{
		[Fact]
		public void CanCustomizeEntityName()
		{
			using(var store = NewDocumentStore())
			{
				store.Conventions.FindTypeTagName = ReflectionUtil.GetFullNameWithoutVersionInformation;

				using(var session = store.OpenSession())
				{
					session.Store(new Foo{Name = "Ayende"});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var typeName = ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(Foo));
					var all = session
                        .Advanced
                        .LuceneQuery<Foo>("Raven/DocumentsByEntityName")
						.Where("Tag:[[" + typeName + "]]")
						.WaitForNonStaleResultsAsOfNow(TimeSpan.MaxValue)
						.ToList();

					Assert.Equal(1, all.Count);
				}

			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}