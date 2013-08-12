// -----------------------------------------------------------------------
//  <copyright file="SpatialMultiPolygon.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class SpatialMultiPolygon : RavenTest
	{
		public class Locale
		{
			public Guid Id { get; set; }
			public string Point { get; set; }
		}

		public class LocaleIndex : AbstractIndexCreationTask<Locale>
		{
			public LocaleIndex()
			{
				Map = entities =>
					  from entity in entities
					  select new
					  {
						  SIdx = SpatialGenerate("Point", entity.Point)
					  };
			}
		}

		[Fact]
		public void ShouldWork()
		{
		 // must be 231 points - return 198 - WRONG (retruns points -180<=x<=-100 and 100<=x<=180, -50<=y<=50)
			const string spatialFilter = "MULTIPOLYGON (((100 50, -100 50, -100 -50, 100 -50, 100 50)))";

			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (var i = -18; i <= 18; i++)
						for (var j = -9; j <= 9; j++)
						{
							var locale = new Locale
							{
								Id = Guid.NewGuid(),
								Point = string.Format("POINT ({0} {1})", i * 10, j * 10)
							};
							session.Store(locale);
						}
					session.SaveChanges();
				}

				 using (var session = store.OpenSession()) {
                var results = new List<Locale>();
                var query = session
                    .Query<Locale, LocaleIndex>()
                    .Customize(x => x.RelatesToShape("Point", spatialFilter, SpatialRelation.Intersects)
                                     .WaitForNonStaleResults());

                List<Locale> resultSet;

                do {
                    resultSet = query.Skip(results.Count).ToList();
                    results.AddRange(resultSet);
                } while (resultSet.Count > 0);

                foreach (var locale in results)
                    Console.WriteLine(locale.Point);

                Console.WriteLine("Number of results:{0}", results.Count);
			}
		}
		}
	}