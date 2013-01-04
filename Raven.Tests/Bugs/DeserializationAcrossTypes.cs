using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	namespace First
	{
		public class Alpha
		{
			public string Foo { get; set; }
		}
	}

	public class DeserializationAcrossTypes : RavenTest
	{
		[Fact]
		public void can_deserialize_across_types_when_origin_type_doesnt_exist()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("alphas/1", null, RavenJObject.Parse("{ 'Foo': 'Bar'}"),
										   RavenJObject.Parse(
											   "{'Raven-Clr-Type': 'Raven.Tests.Bugs.Second.Alpha', 'Raven-Entity-Name': 'Alphas' }"));

				using (var session = store.OpenSession())
				{
					session.Load<First.Alpha>("alphas/1");
				}
			}
		}
	}
}
