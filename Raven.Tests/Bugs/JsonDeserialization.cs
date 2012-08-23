using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class JsonDeserialization : RavenTest
	{
		private class MyEntity
		{
			public object Foo;
		}

		private class AnotherEntity
		{
			public string Bar;
		}

		[Fact]
		public void Should_deserialize_correctly_even_when_the_property_type_is_object()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new MyEntity {Foo = new AnotherEntity {Bar = "test"}});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var myEntity = s.Load<MyEntity>("MyEntities/1");
					Assert.IsType(typeof(AnotherEntity), myEntity.Foo);
				}
			}
		}
	}
}
