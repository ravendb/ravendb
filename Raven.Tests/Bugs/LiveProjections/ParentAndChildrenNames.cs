using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Bugs.LiveProjections
{
	public class ParentAndChildrenNames : AbstractIndexCreationTask<Person>
	{
		public ParentAndChildrenNames()
		{
			Map = people => from person in people
							where person.Children.Length > 0
							select new { person.Name };
			

		}

		public class ParentAndChildrenNamesTransformer : AbstractTransformerCreationTask<Person>
		{
			public ParentAndChildrenNamesTransformer()
			{
				TransformResults = people =>
				from person in people
				let children = LoadDocument<Person>(person.Children)
				select new { person.Name, ChildrenNames = children.Select(x => x.Name) };
			}
		}
	}
}