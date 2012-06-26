// -----------------------------------------------------------------------
//  <copyright file="Arun.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Arun : RavenTest
	{
		public void SaveDynamicEntityWithStronglyTypedProperties()
		{
			RavenQueryStatistics stats = null;
			using (var store = NewDocumentStore())
			{
				new BookSearch().Execute(store);

				Book book = new Book()
				{
					Title = "Greatest Ever",
					Publisher = "DC",
					Contributors = new List<Contributor>()
					{
						new Contributor() {Name = "Jack Sparrow"},
						new Contributor() {Name = "Will Smith"},
						new Contributor() {Name = "Wayne Rooney"}
					},
					Prices = new List<Price>()
					{
						new Price() {price = "10$"},
						new Price() {price = "8£"}
					},
					Subjects = new List<Subject>()
					{
						new Subject() {Code = "1TGS"},
						new Subject() {Code = "8TRD"}
					}
				};

				using (var session = store.OpenSession())
				{
					session.Store(book);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					string[] searchTerms = {"Will", "Wayne", "Jack"};


					var ravenQueryable = session.Query<Book, BookSearch>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Keywords.In(searchTerms));

					var bookinfo = ravenQueryable
						.Statistics(out stats)
						.ToArray();
				}
			}

			Assert.Equal(1, stats.TotalResults);
		}

		public class Book
		{
			public string Keywords { get; set; }


			[DataMember]
			public string Title { get; set; }

			[DataMember]
			public List<Subject> Subjects { get; set; }

			[DataMember]
			public List<Contributor> Contributors { get; set; }

			[DataMember]
			public List<Tag> Tags { get; set; }


			[DataMember]
			public string Publisher { get; set; }

			[DataMember]
			public List<Price> Prices { get; set; }
		}

		public class Subject
		{
			public string Code { get; set; }
		}

		public class Contributor
		{
			public string Name { get; set; }
		}

		public class Price
		{
			public string price { get; set; }
		}

		public class Tag
		{
			public string Name { get; set; }
		}

		public class BookSearch : AbstractIndexCreationTask<Book, BookSearch.Result>
		{
			public BookSearch()
			{
				Map = books => from book in books
				               select new
				               {
				               	Keywords = new object[]
				               	{
				               		book.Title,
				               		book.Contributors.Select(contributor => contributor.Name),
				               		book.Tags.Select(tag => tag.Name),
				               		book.Subjects.Select(subject => subject.Code)
				               	},
				               	Publisher = book.Publisher,
				               	Prices_CountryCodes = book.Prices.Select(price => price.price)
				               };
				Analyzers.Add(book => book.Keywords, "Lucene.Net.Analysis.Standard.StandardAnalyzer");
				Index(book => book.Keywords, FieldIndexing.Analyzed);
			}

			public class Result
			{
				/// <summary>
				/// Gets or sets the keywords for the index for searching the books.
				/// </summary>
				public object[] Keywords { get; set; }


			}
		}
	}
}