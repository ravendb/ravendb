using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Bugs.LiveProjections;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class CreateIndexesWithCasting
	{
		[Fact]
		public void WillPreserverTheCasts()
		{
			var indexDefinition = new WithCasting
			{
				Conventions = new DocumentConvention()	
			}.CreateIndexDefinition();

			Assert.Contains("docs.People.Select(person => new {", indexDefinition.Map);
			Assert.Contains("Id = ((long) person.Name.Length)", indexDefinition.Map);
		}

		public class WithCasting : AbstractIndexCreationTask<Person>
		{
			public WithCasting()
			{
				Map = persons => from person in persons
				                 select new {Id = (long)person.Name.Length};
			}
		}
	}
}