using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList.Apo {
	public class Lazy:RavenTest {

		public class TestClass {
			public string Id { get; set; }

			public string Value { get; set; }

			public DateTime Date { get; set; }
		}

		[Fact]
		public void LazyWhereAndOrderBy() {
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using ( var session = store.OpenSession() ) {
					session.Store( new TestClass() { Id = "testid", Value = "test1", Date = DateTime.UtcNow } );
					session.Store( new TestClass() { Value = "test2", Date = DateTime.UtcNow } );
					session.Store( new TestClass() { Value = "test3", Date = DateTime.UtcNow.AddMinutes( 1 ) } );
					session.SaveChanges();
				}

				using ( var session = store.OpenSession() ) {
					var hello = new List<TestClass>();

					Assert.DoesNotThrow( () => {
						session.Query<TestClass>()
								.Customize(x=>x.WaitForNonStaleResultsAsOfLastWrite())
								.Where(x=> x.Date>=DateTime.UtcNow.AddMinutes(-1))
								.OrderByDescending(x=>x.Date)
								.Lazily( result => {
									hello = result.ToList();
								} );
					} );
				}

			}
		}
	}
}
