using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Issues
{
//	public class RavenDB_1279 : RavenTest
//	{
//		private class Constants
//		{
//			public const string Orders1 = "orders/1";
//			public const string Company85 = "companies/85";
//			public const string Employees1 = "employees/1";
//		}
//
//		public class Orders_ByEmployee
//		{
//		}
//
//		public Raven.Client.IDocumentStore store { get; set; }
//
//		public RavenDB_1279()
//		{
//			store = new DocumentStore { Url = "http://calypso:8080", DefaultDatabase = "Foo" };
//
//			store.Initialize();
//			Raven.Client.Indexes.IndexCreation.CreateIndexes(typeof(Orders_ByEmployee).Assembly, store);
//
//		}
//
//		public override void Dispose()
//		{
//			base.Dispose();
//			if (store != null)
//				store.Dispose();
//		}
//
//		[Fact]
//		public void Load_Company85()
//		{
//			using (var session = store.OpenSession())
//			{
//				var c = session.Load<Orders.Company>(Constants.Company85);
//
//				Assert.NotNull(c);
//
//				Assert.Equal("VINET", c.ExternalId);
//
//			}
//		}
//
//
//		[Fact]
//		public void Load_Order1_Including_Company_Details_In_One_Server_Call()
//		{
//			using (var session = store.OpenSession())
//			{
//				var order = session
//					.Include<Orders.Order>(o => o.Company) // From the order object, get the JSON object associated to the Order.CompanyId Id
//					.Load(Constants.Orders1);// But retrieve the actual Order specified
//
//				Assert.NotNull(order);
//				Assert.Equal(Constants.Company85, order.Company);
//
//				var company = session.Load<Orders.Company>(order.Company);
//
//				Assert.NotNull(company);
//
//				Assert.Equal(order.Company, company.Id);
//				Assert.NotNull(company.Name);
//
//			}
//
//		}
//	}
}
