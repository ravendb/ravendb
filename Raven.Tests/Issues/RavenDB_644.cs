// -----------------------------------------------------------------------
//  <copyright file="RavenDB_644.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System;
	using System.ComponentModel.Composition.Hosting;
	using System.Linq;

	using Raven.Abstractions.Indexing;
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

		public class FancyIndex : AbstractIndexCreationTask<Item, Record>
		{
			public FancyIndex()
			{
				Map = items => from i in items
							   select new
							   {
								   Year = i.Year,
								   Number = i.Number,
								   Count = 0
							   };

				Reduce = records => from r in records
									where r.Number == 10 && r.Year == 2010
									group r by new { r.Year, r.Number }
										into yearAndNumber
										select new
										{
											Year = yearAndNumber.Key.Year,
											Number = yearAndNumber.Key.Number,
											Count = yearAndNumber.Where(x => x.Number == 0).Select(x => yearAndNumber.Count())
										};
			}
		}

		public class ValidFancyIndex : AbstractIndexCreationTask<Item, Record2>
		{
			public ValidFancyIndex()
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
		public void IndexDefinitionBuilderShouldThrow()
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
		public void ServerShouldThrow()
		{
			var exception = Assert.Throws<InvalidOperationException>(
				() =>
				{
					using (var store = NewDocumentStore())
					{
						var container = new CompositionContainer(new TypeCatalog(typeof(Index), typeof(Record)));
						IndexCreation.CreateIndexes(container, store);
					}
				});

			Assert.Equal("Reduce cannot contain Count() methods in grouping.", exception.Message);

			exception = Assert.Throws<InvalidOperationException>(
				() =>
				{
					using (var store = NewDocumentStore())
					{
						var container = new CompositionContainer(new TypeCatalog(typeof(FancyIndex), typeof(Record)));
						IndexCreation.CreateIndexes(container, store);
					}
				});

			Assert.Equal("Reduce cannot contain Count() methods in grouping.", exception.Message);
		}

		[Fact]
		public void ServerShouldThrow2()
		{
			var exception = Assert.Throws<InvalidOperationException>(
				() =>
				{
					using (var store = this.NewRemoteDocumentStore())
					{
						store.DatabaseCommands.PutIndex(
							"Index1",
							new IndexDefinition
							{
								Map = "from i in items select new { Year = i.Year, Number = i.Number, Count = 0 }",
								Reduce =
									"from r in records group r by new { r.Year, r.Number } into yearAndNumber select new { Year = yearAndNumber.Key.Year, Number = yearAndNumber.Key.Number, Count = yearAndNumber.Count() }"
							});
					}
				});

			Assert.Contains("Reduce cannot contain Count() methods in grouping.", exception.Message);

			exception = Assert.Throws<InvalidOperationException>(
				() =>
				{
					using (var store = this.NewRemoteDocumentStore())
					{
						store.DatabaseCommands.PutIndex(
							"Index1",
							new IndexDefinition
							{
								Map = "from i in items select new { Year = i.Year, Number = i.Number, Count = 0 }",
								Reduce =
									"from r in records group r by new { r.Year, r.Number } into yearAndNumber select new { Year = yearAndNumber.Key.Year, Number = yearAndNumber.Key.Number, Count = yearAndNumber.Where(x => x.Number == 0).Select(x => yearAndNumber.Count()) }"
							});
					}
				});

			Assert.Contains("Reduce cannot contain Count() methods in grouping.", exception.Message);
		}

		[Fact]
		public void ServerShouldNotThrow()
		{
			Assert.DoesNotThrow(
				() =>
				{
					using (var store = NewDocumentStore())
					{
						new ValidFancyIndex().Execute(store);
					}
				});

		}
	}
}
