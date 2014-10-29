//-----------------------------------------------------------------------
// <copyright file="LastModifiedRemote.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Metadata
{
	public class EscapeQuotesLocal : RavenTest
	{
		[Fact]
		public void CanProperlyEscapeQuotesInMetadata_Local_1()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var user = new User();
					session.Store(user);
					session.Advanced.GetMetadataFor(user).Add("Foo", "\"Bar\"");
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<User>("users/1");
					var metadata = session.Advanced.GetMetadataFor(user);
					Assert.Equal("\"Bar\"", metadata.Value<string>("Foo"));
				}
			}
		}

		[Fact]
		public void CanProperlyEscapeQuotesInMetadata_Local_2()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var user = new User();
					session.Store(user);
					session.Advanced.GetMetadataFor(user).Add("Foo", "\\\"Bar\\\"");
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<User>("users/1");
					var metadata = session.Advanced.GetMetadataFor(user);
					Assert.Equal("\\\"Bar\\\"", metadata.Value<string>("Foo"));
				}
			}
		}
	}
}
