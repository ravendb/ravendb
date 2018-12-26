using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_12582 : RavenTestBase
    {
        [Fact]
        public void PatchOperationShouldReceiveCompleteInformation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Alexander"
                    }, "users/1");
                    session.SaveChanges();

                    var command = new PatchOperation.PatchCommand(
                        store.Conventions,
                        session.Advanced.Context,
                        "users/1",
                        null,
                        new PatchRequest
                        {
                            Script = @"this.Name = ""Sasha"""
                        },
                        patchIfMissing: null,
                        skipPatchIfChangeVectorMismatch: false,
                        returnDebugInformation: true,
                        test: false);

                    session.Advanced.RequestExecutor.Execute(command, session.Advanced.Context);
                    var result = command.Result;
                    Assert.Equal(PatchStatus.Patched, result.Status);
                    Assert.NotNull(result.ModifiedDocument);
                    Assert.Equal("Sasha", result.ModifiedDocument["Name"].ToString());
                    Assert.NotNull(result.Debug);

                    Assert.NotNull(result.LastModified);
                    Assert.NotNull(result.ChangeVector);
                    Assert.True(result.ChangeVector.Length > 0);
                    Assert.NotNull(result.Collection);
                    Assert.Equal("Users", result.Collection);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Alexander"
                    }, "users/1");
                    session.SaveChanges();

                    var command = new PatchOperation.PatchCommand(
                        store.Conventions,
                        session.Advanced.Context,
                        "users/1",
                        null,
                        new PatchRequest
                        {
                            Script = @"this.Name = ""Alexander"""
                        },
                        patchIfMissing: null,
                        skipPatchIfChangeVectorMismatch: false,
                        returnDebugInformation: false,
                        test: false);

                    session.Advanced.RequestExecutor.Execute(command, session.Advanced.Context);
                    var result = command.Result;
                    Assert.Equal(PatchStatus.NotModified, result.Status);
                    Assert.NotNull(result.ModifiedDocument);
                    Assert.Null(result.Debug);

                    Assert.Null(result.ChangeVector);
                    Assert.Null(result.Collection);
                    Assert.Equal(new DateTime(), result.LastModified);
                }
            }
        }
    }
}




