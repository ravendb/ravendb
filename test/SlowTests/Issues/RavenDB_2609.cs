// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2609.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2609 : RavenTestBase
    {
        [Fact]
        public void ShouldNotOverwriteDocumentIfPatchOpetationDidntModifiedIt()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Company { Name = "Abc" });
                    s.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var companyEtag = commands.Head("companies/1").Value;

                    store.Operations.Send(new PatchOperation("companies/1", null, new PatchRequest
                    {
                        Script = @"this.Name = 'Abc'"
                    }));

                    var afterPatchEtag = commands.Head("companies/1").Value;

                    Assert.Equal(companyEtag, afterPatchEtag);
                }
            }
        }
    }
}
