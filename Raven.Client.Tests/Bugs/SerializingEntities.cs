using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using Newtonsoft.Json;
using Raven.Client.Document;
using Raven.Database;
using Raven.Server;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class SerializingEntities
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
			if (Directory.Exists("Data"))
				Directory.Delete("Data", true);
			try
			{
				using(new RavenDbServer(new RavenConfiguration
				{
					DataDirectory = "Data"
				}))
				using (var documentStore = new DocumentStore
				{
					Url = "http://localhost:8080"
				}.Initialize())
				{
					
					var session = documentStore.OpenSession();

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
			finally
			{
				if (Directory.Exists("Data"))
					Directory.Delete("Data", true);
			}
		}

		[Fact]
		public void WillNotSerializeEvents()
		{
			if (Directory.Exists("Data"))
				Directory.Delete("Data", true);
			try
			{
				using (var documentStore = new DocumentStore())
				{
					documentStore.Configuration.DataDirectory = "Data";
					documentStore.Conventions.CustomizeJsonSerializer = x => x.TypeNameHandling = TypeNameHandling.Auto;
					documentStore.Initialize();

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
			finally
			{
				if (Directory.Exists("Data"))
					Directory.Delete("Data", true);
			}
		}
	}
}