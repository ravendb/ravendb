// -----------------------------------------------------------------------
//  <copyright file="AccessingMetadataInTransformer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class AccessingMetadataInTransformer : RavenTest
	{
		[Fact]
		public void ShouldNotResultInNullReferenceException()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Profile {});
					session.Store(new Profile {});
					session.SaveChanges();
				}

				store.ExecuteTransformer(new Transformer());

				using (var session = store.OpenSession())
				{
					var result = session.Query<Profile>()
					               .Customize(c => c.WaitForNonStaleResults())
					               .TransformWith<Transformer, Transformed>()
					               .ToList();

					var transformed = result.First();
					Assert.True(DateTime.UtcNow - transformed.DateUpdated < TimeSpan.FromSeconds(5), transformed.DateUpdated.ToString("O"));
				}
			}
		}

		private class Transformed
		{
			public DateTime DateUpdated { get; set; }
		}

		private class Transformer : AbstractTransformerCreationTask<Profile>
		{
			public Transformer()
			{
				TransformResults = docs =>
					from doc in docs
					select new Transformed
					{
						DateUpdated = MetadataFor(doc).Value<DateTime>("Last-Modified")
					};
			}
		}

		private class Profile
		{

		}
	}
}