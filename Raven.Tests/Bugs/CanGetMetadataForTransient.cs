//-----------------------------------------------------------------------
// <copyright file="CanGetMetadataForTransient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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
					Assert.True(s.Advanced.GetMetadataFor(entity).Value<bool>("admin"));
				}
			}
		}
	}
}
