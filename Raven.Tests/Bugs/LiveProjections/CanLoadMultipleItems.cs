using System.Linq;
using Raven.Client;
using Xunit;

namespace Raven.Tests.Bugs.LiveProjections
{
	public class CanLoadMultipleItems : LocalClientTest
	{
		[Fact]
		public void CanLoadMultipleItemsInTransformResults()
		{
			using (var store = NewDocumentStore())
			{
				new ParentAndChildrenNames().Execute(((IDocumentStore) store).DatabaseCommands, ((IDocumentStore) store).Conventions);	

				using(var s = store.OpenSession())
				{
					s.Store(new Person
					        	{
					        		Name = "Arava"
					        	});
					s.Store(new Person
					        	{
					        		Name = "Oscar"
					        	});
					s.Store(new Person
					        	{
					        		Name = "Oren",
									Children = new string[] { "people/1" , "people/2"}
					        	});
					s.SaveChanges();

					var results = s.Query<dynamic, ParentAndChildrenNames>().Customize(x=>x.WaitForNonStaleResults())
						.ToArray();

					Assert.Equal(1, results.Length);

					Assert.Equal("Oren", results[0].Name);
					Assert.Equal("Arava", results[0].ChildrenNames[0]);
					Assert.Equal("Oscar", results[0].ChildrenNames[1]);
				}
			}
		}
	}
}