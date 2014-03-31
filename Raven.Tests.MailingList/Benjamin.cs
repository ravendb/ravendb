using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class Benjamin : RavenTest
	{

		public class Person
		{
			public PersonFace Face;
		}

		public class PersonFace
		{
			public string Color { get; set; }
		}


		[Fact]
		public void Can_project_nested_objects()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Person
					{
						Face = new PersonFace
						{
							Color = "Green"
						}
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var faces = session.Query<Person>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Select(x=>x.Face)
						.ToList();

					Assert.Equal("Green", faces[0].Color);
				}
			}
		}
		 
	}
}