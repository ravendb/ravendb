//-----------------------------------------------------------------------
// <copyright file="TotalCountServerTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Document
{
	public class TotalCountServerTest : RavenTest
	{
		[Fact]
		public void TotalResultIsIncludedInQueryResult()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var company1 = new Company
					{
						Name = "Company1",
						Address1 = "",
						Address2 = "",
						Address3 = "",
						Contacts = new List<Contact>(),
						Phone = 2
					};
					var company2 = new Company
					{
						Name = "Company2",
						Address1 = "",
						Address2 = "",
						Address3 = "",
						Contacts = new List<Contact>(),
						Phone = 2
					};

					session.Store(company1);
					session.Store(company2);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
                    int resultCount = session.Advanced.DocumentQuery<Company>().WaitForNonStaleResults().QueryResult.TotalResults;
					Assert.Equal(2, resultCount);
				}
			}
		}
	}
}