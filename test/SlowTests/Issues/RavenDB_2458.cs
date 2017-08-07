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
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.ServerWide.Operations;
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
                var functions = "exports.test = function(value) { return 'test ' + value; };";
                await store.Admin.Server.SendAsync(new ModifyCustomFunctionsOperation(store.Database, functions)).ConfigureAwait(false);

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
                    .Send(new PatchOperation("people/1-A", null, new PatchRequest { Script = "this.Name = test(this.Name);" }));

                using (var session = store.OpenSession())
                {
                    var person = session.Load<Person>("people/1-A");
                    Assert.Equal("test Name1", person.Name);
                }
            }
        }

        [Fact]
        public async Task CustomJavascriptFunctionsShouldBeRemovedFromPatcher()
        {
            using (var store = GetDocumentStore())
            {                

                var functions = "exports.test = function(value) { return 'test ' + value; };";
                await store.Admin.Server.SendAsync(new ModifyCustomFunctionsOperation(store.Database, functions)).ConfigureAwait(false);

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
                    .Send(new PatchOperation("people/1-A", null, new PatchRequest { Script = "this.Name = test(this.Name);" }));

                using (var session = store.OpenSession())
                {
                    var person = session.Load<Person>("people/1-A");
                    Assert.Equal("test Name1", person.Name);
                }

                await store.Admin.Server.SendAsync(new ModifyCustomFunctionsOperation(store.Database, string.Empty)).ConfigureAwait(false);


                Assert.Throws<JavaScriptException>(
                    () => store.Operations.Send(new PatchOperation("people/1-A", null, new PatchRequest { Script = "this.Name = test(this.Name);" })));

                using (var session = store.OpenSession())
                {
                    var person = session.Load<Person>("people/1-A");
                    Assert.Equal("test Name1", person.Name);
                }
            }
        }
    }
}
