// -----------------------------------------------------------------------
//  <copyright file="BrunoLopes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class BrunoLopes : RavenTest
	{
		[Fact]
		public void CompositeIdIsSetAfterSaveChanges()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					Company company = new Company();
					company.Id = "country/2/companies/";

					session.Store(company);
					session.SaveChanges();
					Assert.Equal("country/2/companies/1", company.Id);
				}
			}
		}
	}
}