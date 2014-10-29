// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1532.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class RavenDB_1532 : RavenTest
	{
		[Fact]
		public void TestPatch()
		{
			const long longValue = 6351989385753458511;

			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var foo = new Foo {Id = "foos/1", LongValue = longValue, Baz = "hello"};
					session.Store(foo);
					session.SaveChanges();
				}

				store.DatabaseCommands.Patch("foos/1", new[]
				{
					new PatchRequest
					{
						Type = PatchCommandType.Set,
						Name = "Baz",
						Value = "world"
					}
				});

				using (var session = store.OpenSession())
				{
					var foo = session.Load<Foo>("foos/1");
					Assert.Equal("world", foo.Baz);
					Assert.Equal(longValue, foo.LongValue);
				}

				store.DatabaseCommands.Patch("foos/1", new ScriptedPatchRequest
				{
					Script = @"this.Baz = 'there'"
				});

				using (var session = store.OpenSession())
				{
					var foo = session.Load<Foo>("foos/1");
					Assert.Equal("there", foo.Baz);
					Assert.Equal(longValue, foo.LongValue);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public long LongValue { get; set; }
			public string Baz { get; set; }
		}
	}
}