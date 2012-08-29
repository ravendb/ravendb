using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
				using (var sess = store.OpenSession())
				{
					var a1 = new RavenA { SomeProp = "findme" };
					sess.Store(a1);

					sess.Advanced.GetMetadataFor(a1)["METAPROP"] = "metadata";

					sess.SaveChanges();
				}

				using (var sess2 = store.OpenSession())
				{
					var a1 = sess2.Query<RavenA>().Where(a => a.SomeProp == "findme").FirstOrDefault();

					Assert.Equal("metadata", sess2.Advanced.GetMetadataFor(a1)["METAPROP"]);
				}
			}
		}
	}
}
