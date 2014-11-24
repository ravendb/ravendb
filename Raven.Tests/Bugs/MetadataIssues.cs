using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class MetadataIssues : RavenTest
	{
		public class RavenA
		{
			public string SomeProp { get; set; }
		}

		[Fact]
		public void CanWorkWithMetadata()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var a1 = new RavenA { SomeProp = "findme" };
					session.Store(a1);

					session.Advanced.GetMetadataFor(a1)["METAPROP"] = "metadata";

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var a1 = session.Query<RavenA>().Where(a => a.SomeProp == "findme").FirstOrDefault();

					Assert.Equal("metadata", session.Advanced.GetMetadataFor(a1)["METAPROP"]);
				}
			}
		}
	}
}
