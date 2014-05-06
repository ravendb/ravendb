using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_514 : RavenTest
	{
		 [Fact]
		 public void BoostWithLinq()
		 {
			 using(var store = NewDocumentStore())
			 {
				 store.DatabaseCommands.PutIndex("test", new IndexDefinition
				 {
					 Map = "from p in docs.Products select new { p.Price} .Boost(2)"
				 });
			 }
		 }

		 [Fact]
		 public void BoostWithMethod()
		 {
			 using (var store = NewDocumentStore())
			 {
				 store.DatabaseCommands.PutIndex("test", new IndexDefinition
				 {
					 Map = "docs.Products.Select(p =>new { p.Price } .Boost(2))"
				 });
			 }
		 }
	}
}