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
exports.c = function(v) { throw 'oops'; }
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
                    Assert.Equal(@"Unable to execute JavaScript: 
var s = 1234; 
a(s);

Error: 
oops
Stacktrace:
c@customFunctions.js:3
b@customFunctions.js:2
a@customFunctions.js:1
apply@main.js:2
anonymous function@main.js:1", e.Message);
                }
            }
        }
    }
}
