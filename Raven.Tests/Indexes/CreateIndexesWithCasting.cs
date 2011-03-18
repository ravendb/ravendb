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

			Assert.Equal("docs.People\r\n\t.Select(person => new {Id = (System.Int64)(person.Name.Length)})", indexDefinition.Map);
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