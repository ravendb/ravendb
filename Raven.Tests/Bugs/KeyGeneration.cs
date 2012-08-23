//-----------------------------------------------------------------------
// <copyright file="KeyGeneration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class KeyGeneration : RavenTest
	{
		[Fact]
		public void KeysGeneratedFromDifferentSessionsAreConsecutive()
		{
			using(var store = NewDocumentStore())
			{
				var c1 = new Company();
				var c2 = new Company();

				using(var s = store.OpenSession())
				{
					s.Store(c1);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Store(c2);
					s.SaveChanges();
				}


				Assert.Equal("companies/1", c1.Id);
				Assert.Equal("companies/2", c2.Id);
			}
		}
	}
}
