//-----------------------------------------------------------------------
// <copyright file="SerializingEntities.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Embedded;
using Raven.Database.Extensions;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class SerializingEntities : RavenTest
	{
		public class Foo : INotifyPropertyChanged
		{
			public string Id { get; set; }
			public event PropertyChangedEventHandler PropertyChanged;

			public void InvokePropertyChanged(PropertyChangedEventArgs e)
			{
				PropertyChangedEventHandler handler = PropertyChanged;
				if (handler != null) handler(this, e);
			}
		}

		public class Bar
		{
			public string NotSerializable
			{
				get
				{
					throw new Exception("This shouldn't be serialized");
				}
			}
			public void FooChanged(object sender, PropertyChangedEventArgs e)
			{
			}
		}

		public class Product
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public decimal Cost { get; set; }
		}

		public class Order
		{
			public string Id { get; set; }
			public string Customer { get; set; }
			public IList<OrderLine> OrderLines { get; set; }

			public Order()
			{
				OrderLines = new List<OrderLine>();
			}
		}

		public class OrderLine
		{
			public string ProductId { get; set; }
			public int Quantity { get; set; }
		}

		[Fact]
		public void Daniil_CanSaveProperly()
		{
			using (var documentStore = NewRemoteDocumentStore())
			using (var session = documentStore.OpenSession())
			{
				var product = new Product
				{
					Cost = 3.99m,
					Name = "Milk",
				};
				session.Store(product);
				session.SaveChanges();

				session.Store(new Order
				{
					Customer = "customers/ayende",
					OrderLines =
					{
						new OrderLine
						{
							ProductId = product.Id,
							Quantity = 3
						},
					}
				});
				session.SaveChanges();

			}
		}

		[Fact]
		public void WillNotSerializeEvents()
		{
			using (var documentStore = NewDocumentStore(runInMemory: false, configureStore: store => store.Conventions.CustomizeJsonSerializer = x => x.TypeNameHandling = TypeNameHandling.Auto))
			{
				var bar = new Bar();
				var foo = new Foo();
				foo.PropertyChanged += bar.FooChanged;

				using (var session = documentStore.OpenSession())
				{
					session.Store(foo);
					session.SaveChanges();
				}
			}
		}
	}
}