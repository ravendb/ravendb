//-----------------------------------------------------------------------
// <copyright file="CanUseNonStringsForId.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CanUseNonStringsForId : RavenTest
	{
		[Fact]
		public void CanStoreAndLoadEntityWithIntKey()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new UserInt32
					{
						Id = 3,
						Name = "Ayende"
					});
					s.SaveChanges();
				}

				using(var s = store.OpenSession())
				{
					var userInt32 = s.Load<UserInt32>("userint32s/3");
					Assert.Equal(3, userInt32.Id);
					Assert.Equal("Ayende", userInt32.Name);
				}
			}
		}

		[Fact]
		public void CanStoreAndLoadEntityWithGuidKey()
		{
			var id = Guid.NewGuid();
			using (var store = NewDocumentStore())
			{
				store.Conventions.FindFullDocumentKeyFromNonStringIdentifier = (o, type, allowNull) => o.ToString();
				using (var s = store.OpenSession())
				{
					s.Store(new UserGuid()
					{
						Id = id,
						Name = "Ayende"
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var user = s.Load<UserGuid>(id.ToString());
					Assert.Equal(id, user.Id);
					Assert.Equal("Ayende", user.Name);
				}
			}
		}
	}
}
