// -----------------------------------------------------------------------
//  <copyright file="IdsaTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.MailingList.Idsa
{
	public class IdsaTest : RavenTest
	{
		[Fact]
		public void CanGetEmptyCollection()
		{
			using (var store = NewDocumentStore())
			{
				new CasinosSuspensionsIndex().Execute(store);

				using (var documentSession = store.OpenSession())
				{

					var casino = new Casino("cities/1", "address", "name")
					{
						Suspensions = new List<Suspension>()
						{
							new Suspension(DateTime.UtcNow, new List<Exemption>())
						}
					};
					documentSession.Store(casino);
					documentSession.SaveChanges();

					var suspensions = documentSession.Query<CasinosSuspensionsIndex.IndexResult, CasinosSuspensionsIndex>().
						Customize(x=>x.WaitForNonStaleResults()).
						Where(x => x.CityId == "cities/1").
						OrderByDescending(x => x.DateTime).
						Take(10).
						AsProjection<CasinosSuspensionsIndex.IndexResult>().
						ToList();

					Assert.True(suspensions.All(x => x.Exemptions != null));
				}
			}
		}
	}
}