// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3237.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using FastTests;
using Microsoft.CSharp.RuntimeBinder;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes.Static;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3237 : RavenTestBase
    {
        public RavenDB_3237(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CaseOne()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands
                        .Put("keys/1", null, new { Test = new[] { 7 } });

                    store
                        .Operations
                        .Send(new PatchOperation("keys/1", null, new PatchRequest { Script = "var a = 1;" }));

                    var doc = commands.Get("keys/1");

                    Assert.NotNull(doc);

                    AssertDynamic(doc, d =>
                    {
                        dynamic test = (DynamicArray)d.Test;

                        Assert.Equal(1, test.Length);
                        Assert.Equal(7, test[0]);
                    });
                }
            }
        }

        [Fact]
        public void CaseTwo()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands
                        .Put("keys/1", null, new { Test = new[] { 3 }, Test2 = new[] { 7 } });

                    store
                        .Operations
                        .Send(new PatchOperation("keys/1", null, new PatchRequest { Script = "this.Test.push(4);" }));

                    var doc = commands.Get("keys/1");

                    Assert.NotNull(doc);

                    AssertDynamic(doc, d =>
                    {
                        dynamic test = (DynamicArray)d.Test;

                        Assert.Equal(2, test.Length);
                        Assert.Equal(3, test[0]);
                        Assert.Equal(4, test[1]);

                        dynamic test2 = (DynamicArray)d.Test2;

                        Assert.Equal(1, test2.Length);
                        Assert.Equal(7, test2[0]);
                    });
                }
            }
        }

        [Fact]
        public void CaseThree()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands
                        .Put("keys/1", null, new { Test = new[] { 3 }, Test2 = new[] { "7" } });

                    store
                        .Operations
                        .Send(new PatchOperation("keys/1", null, new PatchRequest { Script = "this.Test.push(4);" }));

                    var doc = commands.Get("keys/1");

                    Assert.NotNull(doc);

                    AssertDynamic(doc, d =>
                    {
                        dynamic test = (DynamicArray)d.Test;

                        Assert.Equal(2, test.Length);
                        Assert.Equal(3, test[0]);
                        Assert.Equal(4, test[1]);

                        dynamic test2 = (DynamicArray)d.Test2;

                        Assert.Equal(1, test2.Length);
                        Assert.Equal("7", test2[0]);
                    });
                }
            }
        }

        private void AssertDynamic(DynamicBlittableJson json, Action<dynamic> assert)
        {
            try
            {
                dynamic doc = json;
                assert(doc);
            }
            catch (RuntimeBinderException e)
            {
                throw new InvalidOperationException("Failed to assert: " + json.BlittableJson.ToString(), e);
            }
        }
    }
}
