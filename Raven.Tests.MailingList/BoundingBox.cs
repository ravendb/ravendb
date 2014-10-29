// -----------------------------------------------------------------------
//  <copyright file="BoundingBox.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class BoundingBox : RavenTest
	{
		[Fact]
		public void ShouldGetRightResults()
		{
			// verify using http://arthur-e.github.io/Wicket/sandbox-gmaps3.html
			string outer = "POLYGON((-2.14 53.0,-2.14 53.6,-1.52 53.6,-1.52 53.0,-2.14 53.0))";
			string inner = "POLYGON((-1.778 53.205,-1.778 53.207,-1.776 53.207,-1.776 53.205,-1.778 53.205))";

			using (EmbeddableDocumentStore documentStore = NewDocumentStore())
			{
				new Shapes_SpatialIndex().Execute(documentStore);
				using (IDocumentSession db = documentStore.OpenSession())
				{
					var shape = new Shape {Wkt = inner,};
					db.Store(shape);
					db.SaveChanges();
				}

				WaitForIndexing(documentStore);
				using (IDocumentSession db = documentStore.OpenSession())
				{
					List<Shape> results = db.Query<Shape, Shapes_SpatialIndex>()
					                        .Customize(x => x.RelatesToShape("Bbox", outer, SpatialRelation.Within))
					                        .ToList(); // hangs

					Assert.Equal(1, results.Count);
				}
			}
		}

		public class Shape
		{
			public int Id { get; set; }
			public string Wkt { get; set; }
		}

		public class Shapes_SpatialIndex : AbstractIndexCreationTask<Shape>
		{
			public Shapes_SpatialIndex()
			{
				Map = shapes => from s in shapes
				                select new
				                {
					                __ = SpatialGenerate("Bbox", s.Wkt, SpatialSearchStrategy.GeohashPrefixTree, 6)
				                };
			}
		}
	}
}