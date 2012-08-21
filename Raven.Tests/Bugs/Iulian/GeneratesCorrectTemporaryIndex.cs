
namespace Raven.Tests.Bugs.Iulian
{
	using System.Linq;
	using Xunit;

	public class GeneratesCorrectTemporaryIndex : RavenTest
	{
		public class Inner
		{
			public bool Flag { get; set; }
		}

		public class Outer
		{
			public Inner Inner { get; set; }
		}

		[Fact]
		public void Can_Generate_Correct_Temporary_Index()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					// store the element
					Outer outer = new Outer { Inner = new Inner { Flag = true } };
					s.Store(outer);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					// query by the inner flag
					Outer outer = s.Query<Outer>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(o => o.Inner.Flag).SingleOrDefault();

					Assert.NotNull(outer); // this fails
				}
			}
		}
	}
}
