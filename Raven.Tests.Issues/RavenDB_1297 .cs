// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1297 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1297 : RavenTest
	{
		[Fact]
		public void QueryingAutoIndexOnDictionaryShouldNotFailIfFirstQueryIncludeKeyOnlyAndSecondIncludesValueToo()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var foo = new Foo();
					session.Store(foo, "foos/1");
					session.SaveChanges();

					session.Query<Foo>()
					       .Customize(c => c.WaitForNonStaleResults())
					       .Where(it => it.Context.Any(pair => pair.Key == "Foo"))
					       .ToList();

					// this will fail because it tries to add Key twice
					session.Query<Foo>()
						   .Customize(c => c.WaitForNonStaleResults())
						   .Where(it => it.Context.Any(pair => pair.Key == "Foo" && pair.Value == "Bar"))
						   .ToList();
				}
			}
		}

		public class Foo
		{
			public Dictionary<string, string> Context { get; set; }

			public Foo()
			{
				Context = new Dictionary<string, string>
				{
					{"Foo", "Bar"}
				};
			}
		}
	}
}