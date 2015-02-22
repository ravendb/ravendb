// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3237.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3237 : RavenTest
	{
		[Fact]
		public void CaseOne()
		{
			using (var store = NewDocumentStore())
			{
				store
					.DatabaseCommands
					.Put("keys/1", null, RavenJObject.FromObject(new { Test = new[] { 7 } }), new RavenJObject());

				store
					.DatabaseCommands
					.Patch("keys/1", new ScriptedPatchRequest { Script = "var a = 1;" });

				var doc = store.DatabaseCommands.Get("keys/1");

				Assert.NotNull(doc);

				var test = (RavenJArray)doc.DataAsJson["Test"];

				Assert.Equal(1, test.Length);
				Assert.Equal(7, test[0]);
			}
		}

		[Fact]
		public void CaseTwo()
		{
			using (var store = NewDocumentStore())
			{
				store
					.DatabaseCommands
					.Put("keys/1", null, RavenJObject.FromObject(new { Test = new[] { 3 }, Test2 = new[] { 7 } }), new RavenJObject());

				store
					.DatabaseCommands
					.Patch("keys/1", new ScriptedPatchRequest { Script = "this.Test.push(4);" });

				var doc = store.DatabaseCommands.Get("keys/1");

				Assert.NotNull(doc);

				var test = (RavenJArray)doc.DataAsJson["Test"];

				Assert.Equal(2, test.Length);
				Assert.Equal(3, test[0]);
				Assert.Equal(4, test[1]);

				var test2 = (RavenJArray)doc.DataAsJson["Test2"];

				Assert.Equal(1, test2.Length);
				Assert.Equal(7, test2[0]);
			}
		}

		[Fact]
		public void CaseThree()
		{
			using (var store = NewDocumentStore())
			{
				store
					.DatabaseCommands
					.Put("keys/1", null, RavenJObject.FromObject(new { Test = new[] { 3 }, Test2 = new[] { "7" } }), new RavenJObject());

				store
					.DatabaseCommands
					.Patch("keys/1", new ScriptedPatchRequest { Script = "this.Test.push(4);" });

				var doc = store.DatabaseCommands.Get("keys/1");

				Assert.NotNull(doc);

				var test = (RavenJArray)doc.DataAsJson["Test"];

				Assert.Equal(2, test.Length);
				Assert.Equal(3, test[0]);
				Assert.Equal(4, test[1]);

				var test2 = (RavenJArray)doc.DataAsJson["Test2"];

				Assert.Equal(1, test2.Length);
				Assert.Equal("7", test2[0]);
			}
		}
	}
}