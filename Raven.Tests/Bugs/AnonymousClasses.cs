//-----------------------------------------------------------------------
// <copyright file="AnonymousClasses.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class AnonymousClasses : RavenTest
	{
		[Fact]
		public void WillNotCreateNastyIds()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					var entity = new {a = 1};
					s.Store(entity);
					s.SaveChanges();

					string id = s.Advanced.GetDocumentId(entity);

					Assert.DoesNotContain("anonymoustype", id);
				}
			}
		}

		[Fact]
		public void WillNotSetRavenEntityName()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					var entity = new { a = 1 };
					s.Store(entity);
					s.SaveChanges();

					var metadata = s.Advanced.GetMetadataFor(entity);

					Assert.Null(metadata["Raven-Entity-Name"]);
				}
			}
		}
	}
}
