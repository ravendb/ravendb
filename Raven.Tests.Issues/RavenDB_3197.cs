// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3197.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Bundles.ScriptedIndexResults;
using Raven.Database.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	
	public class RavenDB_3197 : RavenTest
	{

		public class SimpleUser
		{
			public string FirstName { get; set; }
			public string LastName { get; set; }
		}


		[Fact]
		public void ScriptPatchShouldGenerateNiceException()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new SimpleUser { FirstName = "John", LastName = "Smith"});
					session.SaveChanges();
				}

				store
					.DatabaseCommands
					.Put(
						Constants.RavenJavascriptFunctions,
						null,
						RavenJObject.FromObject(new { Functions =
@"exports.a = function(value) { return  b(value); };
exports.b = function(v) { return c(v); }
exports.c = function(v) { return v.noSuch.noSuch; }
"
						}),
						new RavenJObject());

				WaitForIndexing(store);

				var patcher = new ScriptedJsonPatcher(store.SystemDatabase);
				using (var scope = new ScriptedIndexResultsJsonPatcherScope(store.SystemDatabase, new HashSet<string>()))
				{
					var e = Assert.Throws<InvalidOperationException>(() => patcher.Apply(scope, new RavenJObject(), new ScriptedPatchRequest
					{
						Script = @"var s = 1234; 
a(s);"
					}));
					Assert.Equal("Unable to execute JavaScript: " + Environment.NewLine +
						"var s = 1234; " + Environment.NewLine +
						"a(s);" + Environment.NewLine + 
						"Error: " + Environment.NewLine + 
						"TypeError: noSuch is undefined" + Environment.NewLine + 
						"Stacktrace:" + Environment.NewLine + 
						"c@customFunctions.js:3" + Environment.NewLine + 
						"b@customFunctions.js:2" + Environment.NewLine +
						"a@customFunctions.js:1" + Environment.NewLine +
						"apply@main.js:2" + Environment.NewLine +
						"anonymous function@main.js:1", e.Message);
				}
			}
		}
	}
}