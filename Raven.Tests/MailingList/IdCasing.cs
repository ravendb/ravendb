// -----------------------------------------------------------------------
//  <copyright file="IdCasing.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class IdCasing : RavenTest
	{
		public class Mutator
		{
			public string Id { get; set; }
			public bool Deleted { get; set; }
			public int Order { get; set; }
		}

		public class Car : Mutator
		{
			public string Brand { get; set; }
		}

		[Fact]
		public void ShouldNotChange()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Car
					{
						Id = "Bmw_z3_2009",
						Brand = "Test"
					});

					session.SaveChanges();

					var car = session.Load<Car>("BMW_z3_2009");
					Assert.True(String.CompareOrdinal(car.Id, "Bmw_z3_2009") == 0);

				}

				using (var session = store.OpenSession())
				{
					var car = session.Load<Car>("BMW_z3_2009");
					Assert.True(String.CompareOrdinal(car.Id, "Bmw_z3_2009") == 0);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var car = session.Load<Car>("BMW_z3_2009");
					Assert.True(String.CompareOrdinal(car.Id, "Bmw_z3_2009") == 0);
				}
			}
		}

		[Fact]
		public void ShouldNotChange_remote()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Car
					{
						Id = "Bmw_z3_2009",
						Brand = "Test"
					});

					session.SaveChanges();

					var car = session.Load<Car>("BMW_z3_2009");
					Assert.True(String.CompareOrdinal(car.Id, "Bmw_z3_2009") == 0);
				}

				using (var session = store.OpenSession())
				{
					var car = session.Load<Car>("BMW_z3_2009");
					Assert.True(String.CompareOrdinal(car.Id, "Bmw_z3_2009") == 0);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var car = session.Load<Car>("BMW_z3_2009");
					Assert.True(String.CompareOrdinal(car.Id, "Bmw_z3_2009") == 0);
				}
			}
		}
	}
}
