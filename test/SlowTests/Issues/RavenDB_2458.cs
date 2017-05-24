// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2458.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Exceptions.Patching;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2458 : RavenTestBase
    {
        [Fact]
        public async Task CustomJavascriptFunctionsShouldWorkServerSide()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put(
                        Constants.Json.CustomFunctionsId,
                        null,
                        new { Functions = "exports.test = function(value) { return 'test ' + value; };" });
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                Assert.True(SpinWait.SpinUntil(() => database.Patcher.CustomFunctions != null, TimeSpan.FromSeconds(10)));

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Name1"
                    });

                    session.SaveChanges();
                }

                store
                    .Operations
                    .Send(new PatchOperation("people/1", null, new PatchRequest { Script = "this.Name = test(this.Name);" }));

                using (var session = store.OpenSession())
                {
                    var person = session.Load<Person>("people/1");
                    Assert.Equal("test Name1", person.Name);
                }
            }
        }

        [Fact]
        public async Task CustomJavascriptFunctionsShouldBeRemovedFromPatcher()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put(
                        Constants.Json.CustomFunctionsId,
                        null,
                        new { Functions = "exports.test = function(value) { return 'test ' + value; };" });

                    var database = await GetDocumentDatabaseInstanceFor(store);
                    Assert.True(SpinWait.SpinUntil(() => database.Patcher.CustomFunctions != null, TimeSpan.FromSeconds(10)));

                    using (var session = store.OpenSession())
                    {
                        session.Store(new Person
                        {
                            Name = "Name1"
                        });

                        session.SaveChanges();
                    }

                    store
                        .Operations
                        .Send(new PatchOperation("people/1", null, new PatchRequest { Script = "this.Name = test(this.Name);" }));

                    using (var session = store.OpenSession())
                    {
                        var person = session.Load<Person>("people/1");
                        Assert.Equal("test Name1", person.Name);
                    }

                    commands
                        .Delete(Constants.Json.CustomFunctionsId, null);

                    Assert.True(SpinWait.SpinUntil(() => database.Patcher.CustomFunctions == null, TimeSpan.FromSeconds(10)));

                    Assert.Throws<JavaScriptException>(
                        () => store.Operations.Send(new PatchOperation("people/1", null, new PatchRequest { Script = "this.Name = test(this.Name);" })));

                    using (var session = store.OpenSession())
                    {
                        var person = session.Load<Person>("people/1");
                        Assert.Equal("test Name1", person.Name);
                    }
                }
            }
        }
    }
}
