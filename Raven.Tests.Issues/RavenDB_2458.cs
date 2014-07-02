// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2458.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2458 : RavenTest
	{
		[Fact]
		public void CustomJavascriptFunctionsShouldWorkServerSide()
		{
			using (var store = NewDocumentStore())
			{
				store
					.DatabaseCommands
					.Put(
						Constants.RavenJavascriptFunctions,
						null,
						RavenJObject.FromObject(new { Functions = "exports.test = function(value) { return 'test ' + value; };" }),
						new RavenJObject());

				using (var session = store.OpenSession())
				{
					session.Store(new Person
								  {
									  Name = "Name1"
								  });

					session.SaveChanges();
				}

				store
					.DatabaseCommands
					.Patch("people/1", new ScriptedPatchRequest { Script = "this.Name = test(this.Name);" });

				using (var session = store.OpenSession())
				{
					var person = session.Load<Person>("people/1");
					Assert.Equal("test Name1", person.Name);
				}
			}
		}

		[Fact]
		public void CustomJavascriptFunctionsShouldBeRemovedFromPatcher()
		{
			using (var store = NewDocumentStore())
			{
				store
					.DatabaseCommands
					.Put(
						Constants.RavenJavascriptFunctions,
						null,
						RavenJObject.FromObject(new { Functions = "exports.test = function(value) { return 'test ' + value; };" }),
						new RavenJObject());

				using (var session = store.OpenSession())
				{
					session.Store(new Person
					{
						Name = "Name1"
					});

					session.SaveChanges();
				}

				store
					.DatabaseCommands
					.Patch("people/1", new ScriptedPatchRequest { Script = "this.Name = test(this.Name);" });

				using (var session = store.OpenSession())
				{
					var person = session.Load<Person>("people/1");
					Assert.Equal("test Name1", person.Name);
				}

				store
					.DatabaseCommands
					.Delete(Constants.RavenJavascriptFunctions, null);

				Assert.Throws<ErrorResponseException>(() => store
					.DatabaseCommands
					.Patch("people/1", new ScriptedPatchRequest { Script = "this.Name = test(this.Name);" }));
				
				using (var session = store.OpenSession())
				{
					var person = session.Load<Person>("people/1");
					Assert.Equal("test Name1", person.Name);
				}
			}
		}
	}
}