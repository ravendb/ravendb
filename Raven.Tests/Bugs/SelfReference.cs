using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SelfReference : RavenTest
	{
		[JsonObject(IsReference = true)] 
		public class Foo
		{
			// No default ctor!!!
			public Foo(string something)
			{
			}

			public Foo OtherFoo { get; set; }
		}

		public class Bar
		{
			public string Id { get; set; }

			public Foo MyFoo { get; private set; }

			public Bar()
			{
				MyFoo = new Foo("something");
			}
		}

		[Fact]
		public void Should_deserialize_correctly()
		{
			var rjson = RavenJToken.FromObject(new Bar()).ToString();
			var robj = RavenJToken.Parse(rjson);

			var json = JToken.FromObject(new Bar()).ToString();
			var obj = JToken.Parse(json);

			Assert.Equal(json, rjson);

			using (var store = NewDocumentStore())
			{
				string id;
				using (var session = store.OpenSession())
				{
					var bar = new Bar();
					session.Store(bar);
					session.SaveChanges();
					id = bar.Id;
				}

				using (var session = store.OpenSession())
				{
					var bar = session.Load<Bar>(id);
					Assert.NotNull(bar);
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var bar = session.Query<Bar>().FirstOrDefault();
					Assert.NotNull(bar);
				}
			}
		}
	}
}
