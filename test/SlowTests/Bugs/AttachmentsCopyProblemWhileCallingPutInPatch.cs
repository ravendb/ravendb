using System;
using System.IO;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Bugs
{
    public class AttachmentsCopyProblemWhileCallingPutInPatch : RavenTestBase
    {
        [Fact]
        public void PatchByQuery()
        {
            var expectedAttachmentStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6 });
            var employee = new Employee
            {
                FirstName = "Avi"
            };
            const string newId = "second/1-A";
            const string attachmentName = "Profile Picture";

            bool doHaveAttachments;
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(employee);

                    session.Advanced.Attachments.Store(employee.Id, attachmentName, expectedAttachmentStream);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Get(employee.Id, attachmentName);
                }

                store.Operations.Send(new PatchByQueryOperation(new IndexQuery()
                {
                    Query = $"from Employees update {{ put('{newId}', this); }}"
                })).WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var newEmployee = session.Load<Employee>(newId);
                    var newEmployeeMetadata = session.Advanced.GetMetadataFor(newEmployee);
                    doHaveAttachments = newEmployeeMetadata.TryGetValue(Constants.Documents.Metadata.Attachments, out object _);
                }
            }

            //Assert
            Assert.False(doHaveAttachments, "The new employee should have no attachment properties in metadata");
        }

        [Fact]
        public void PatchByBatch()
        {
            var expectedAttachmentStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5, 6 });
            var employee = new Employee
            {
                FirstName = "Avi"
            };
            const string newId = "second/1-A";
            const string attachmentName = "Profile Picture";

            bool doHaveAttachments;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(employee);

                    session.Advanced.Attachments.Store(employee.Id, attachmentName, expectedAttachmentStream);

                    session.SaveChanges();

                }

                using (var session = store.OpenSession())
                {
                    var localEmployee = session.Load<Employee>(employee.Id);
                    var changeVector = session.Advanced.GetChangeVectorFor(localEmployee);

                    session.Advanced.Defer(new PatchCommandData(
                        id: employee.Id,
                        changeVector: changeVector,
                        patch: new PatchRequest
                        {
                            Script = $"put('{newId}', this)"
                        },
                        patchIfMissing: null));

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var newEmployee = session.Load<Employee>(newId);
                    var newEmployeeMetadata = session.Advanced.GetMetadataFor(newEmployee);
                    doHaveAttachments = newEmployeeMetadata.TryGetValue(Constants.Documents.Metadata.Attachments, out object _);
                }
            }

            //Assert
            Assert.False(doHaveAttachments, "The new employee should have no attachment properties in metadata");
        }
    }
}
