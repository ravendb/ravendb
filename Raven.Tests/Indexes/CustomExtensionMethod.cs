using System.Diagnostics;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Indexes
{
	public class CustomExtensionMethod : NoDisposalNeeded
	{
		[Fact]
		public void Custom_Extension_Method_Is_Translated_As_Method_Call()
		{
			var index = new TestIndex();
			var map = index.CreateIndexDefinition().Map;
			Debug.WriteLine(map);
			Assert.Contains("Baz = CustomExtensionMethods.DoSomething(foo.Bar)", map);
		}

		public class TestIndex : AbstractIndexCreationTask<Foo>
		{
			public TestIndex()
			{
				Map = foos => from foo in foos
							  select new { Baz = foo.Bar.DoSomething() };
			}
		}

		public class Foo
		{
			public string Bar { get; set; }
		}
	}

	public static class CustomExtensionMethods
	{
		[RavenMethod]
		public static string DoSomething(this string s)
		{
			// implementation details are not important.
			// The real implementation would have to come from a bundle anyway.
			return s;
		}
	}
}