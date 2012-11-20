// -----------------------------------------------------------------------
//  <copyright file="RavenDB_644.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System;
	using System.Linq;
	using Raven.Client.Indexes;

	using Xunit;

	public class RavenDB_644 : RavenTest
	{
		 public class Item
		 {
			 public int Year { get; set; }

			 public int Number { get; set; }
		 }

		public class Record
		{
			public int Year { get; set; }

			public int Number { get; set; }

			public int Count { get; set; }
		}

		public class Record2
		{
			public int Year { get; set; }

			public int Number { get; set; }

			public Subrecord Count { get; set; }
		}

		public class Subrecord
		{
			public int Number { get; set; }
			public int Count { get; set; }
		}

		public class Index : AbstractIndexCreationTask<Item, Record>
		{
			public Index()
			{
				Map = items => from i in items
							   select new
									  {
										  Year = i.Year,
										  Number = i.Number,
										  Count = 0
									  };

				Reduce = records => from r in records
									group r by new { r.Year, r.Number }
									into yearAndNumber
									select new
									{
										Year = yearAndNumber.Key.Year,
										Number = yearAndNumber.Key.Number,
										Count = yearAndNumber.Count()
									};
			}
		}

		public class Index2 : AbstractIndexCreationTask<Item, Record2>
		{
			public Index2()
			{
				Map = items => from i in items
							   select new
							   {
								   Year = i.Year,
								   Number = i.Number,
								   Count =  new { i.Number, Count = 1 }
							   };

				Reduce = records => from r in records
				                    group r by new {r.Year, r.Number}
				                    into yearAndNumber
				                    select new
				                    {
					                    Year = yearAndNumber.Key.Year,
					                    Number = yearAndNumber.Key.Number,
					                    Count = yearAndNumber.GroupBy(x => x.Number)
					                    .Select(g => new
					                    {
						                    Number = g.Key,
						                    Count = g.Count()
					                    })
				                    };
			}
		}

		[Fact]
		public void T1()
		{
			var exception = Assert.Throws<InvalidOperationException>(
				() =>
				{
					using (var store = NewDocumentStore())
					{
						new Index().Execute(store);
					}
				});

			Assert.Equal("Reduce cannot contain Count() methods in grouping.", exception.Message);
		}

		[Fact]
		public void T2()
		{
			Assert.DoesNotThrow(
				() =>
				{
					using (var store = NewDocumentStore())
					{
						new Index2().Execute(store);
					}
				});

		}
	}
}