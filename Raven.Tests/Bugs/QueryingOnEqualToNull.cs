//-----------------------------------------------------------------------
// <copyright file="QueryingOnEqualToNull.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Tests.Document;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class QueryingOnEqualToNull : RavenTest
	{
		[Fact]
		public void QueryingOnEqNull()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new Company
					{
						Phone = 1,
						Type = Company.CompanyType.Public,
						Name = null
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					int actual = s.Query<Company>().Where(x => x.Name == null).Count();
					Assert.Equal(1, actual);
				}
			}
		}
	}
}