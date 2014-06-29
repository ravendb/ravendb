// -----------------------------------------------------------------------
//  <copyright file="RavenDB2408.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB2408 : RavenTest
    {
        [Fact]
        public void Document_id_not_null_when_projecting()
        {
            using (var store = NewDocumentStore())
            {
                new TimeoutsIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Timeout
                    {
                        Owner = String.Empty,
                        Time = DateTime.UtcNow,
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Timeout, TimeoutsIndex>()
                        .Where(t => t.Owner == String.Empty)
                        .OrderBy(t => t.Time)
                        .Select(t => t.Time);

                    QueryHeaderInformation qhi;
                    using (var enumerator = session.Advanced.Stream(query, out qhi))
                    {
                        while (enumerator.MoveNext())
                        {
                            Assert.NotNull(enumerator.Current.Key);
                        }
                    }
                }
            }
        }

        public class Timeout
        {
            public string Id { get; set; }
            public DateTime Time { get; set; }
            public string Owner { get; set; }
        }

        public class TimeoutsIndex : AbstractIndexCreationTask<Timeout>
        {
            public TimeoutsIndex()
            {
                Map = docs => from d in docs
                              select new Timeout { Owner = d.Owner, Time = d.Time };
            }
        }


		public class OrdersByCompany : AbstractIndexCreationTask
		{
			public override string IndexName
			{
				get { return "OrdersByCompany"; }
			}

			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinition
				{
					Maps = { @"from order in docs.Orders
						select new { order.Company, Count = 1, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }", "123" },
					Reduce = @"",
					MaxIndexOutputsPerDocument = 3,

/*
           
			Indexes = new Dictionary<string, FieldIndexing>();
			Analyzers = new Dictionary<string, string>();
			SortOptions = new Dictionary<string, SortOptions>();
			Suggestions = new Dictionary<string, SuggestionOptions>();
			TermVectors = new Dictionary<string, FieldTermVector>();
			SpatialIndexes = new Dictionary<string, SpatialOptions>();


            Fields = new List<string>();*/


					Stores = { { "Total", FieldStorage.Yes }, { "TTT", FieldStorage.No } }
				};
			}
		}

    }
}