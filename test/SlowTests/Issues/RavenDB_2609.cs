// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2609.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class EntityWithNullableAndOrginaryValues
    {
        public string Name { get; set; }

        public decimal SomeDecimal { get; set; }
        public int SomeInt { get; set; }
        public float SomeFloat { get; set; }
        public double SomeDouble { get; set; }
        public long SomeLong { get; set; }

        public decimal? SomeNullableDecimal { get; set; }
        public int? SomeNullableInt { get; set; }
        public float? SomeNullableFloat { get; set; }
        public double? SomeNullableDouble { get; set; }
        public long? SomeNullableLong { get; set; }

        public List<string> SomeArray { get; set; }


    }
    public class RavenDB_2609 : RavenTestBase
    {
        [Fact]
        public void ShouldNotOverwriteDocumentIfPatchOperationDidNotModifiedIt()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new EntityWithNullableAndOrginaryValues
                    {
                        Name = "Abc",

                    }, "entities/1");
                    s.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var companyEtag = commands.Head("entities/1");

                    store.Operations.Send(new PatchOperation("companies/1", null, new PatchRequest
                    {
                        Script = @"this.Name = 'Abc';"
                    }));

                    var afterPatchEtag = commands.Head("entities/1");

                    Assert.Equal(companyEtag, afterPatchEtag);
                }
            }
        }

        [Fact]
        public void ShouldNotOverwriteDocumentIfPatchOperationDidNotModifyItButPutDefaultValues()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new EntityWithNullableAndOrginaryValues
                    {
                        Name = "Abc",

                    }, "entities/1");
                    s.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var companyEtag = commands.Head("entities/1");

                    store.Operations.Send(new PatchOperation("entities/1", null, new PatchRequest
                    {

                        Script = @"this.Name = 'Abc'; 
this.SomeDecimal=0;
this.SomeDouble=0;
this.SomeFloat=0;
this.SomeInt=0;
this.SomeNullableDecimal = null;
this.SomeNullableInt = null;
this.SomeNullableDouble = null;
this.SomeNullableLong = null;
this.SomeArray = null;"
                    }));

                    var afterPatchEtag = commands.Head("entities/1");

                    Assert.Equal(companyEtag, afterPatchEtag);
                }
            }
        }
    }
}
