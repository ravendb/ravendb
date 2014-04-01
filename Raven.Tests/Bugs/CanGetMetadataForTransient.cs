//-----------------------------------------------------------------------
// <copyright file="CanGetMetadataForTransient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class CanGetMetadataForTransient : RavenTest
	{
		[Fact]
		public void GetMetadataForTransient()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					var entity = new User{Name = "Ayende"};
					s.Store(entity);
					s.Advanced.GetMetadataFor(entity)["admin"] = true;

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var entity = new User{Id = "users/1"};
					var metadata = s.Advanced.GetMetadataFor(entity);
					Assert.True(metadata.Value<bool>("Admin")); // metadata values are uppercased
				}
			}
		}
	}
}
