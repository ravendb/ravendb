using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.MailingList
{
    public class When_trying_to_load_a_non_existing_document : RavenTestBase
    {
        [Fact]
        public async Task Then_null_should_be_returned()
        {
            var id = "students/1";

            using (var store = GetDocumentStore())
            {
                new StudentViewModelTransformer().Execute(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(new Student { Id = id, Email = "support@hibernatingrhinos.com" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    StudentViewModel studentVm = session.Load<StudentViewModelTransformer, StudentViewModel>(id);
                    Assert.NotNull(studentVm);

                    var nonExistingKey = "students/2";

                    studentVm = session.Load<StudentViewModelTransformer, StudentViewModel>(nonExistingKey);
                    Assert.Null(studentVm);
                }

                using (var session = store.OpenAsyncSession())
                {

                    StudentViewModel studentVm = await session.LoadAsync<StudentViewModelTransformer, StudentViewModel>(id);
                    Assert.NotNull(studentVm);

                    studentVm = await session.LoadAsync<StudentViewModelTransformer, StudentViewModel>("students/2");
                    Assert.Null(studentVm);
                }
            }
        }

        private class Student
        {
            public string Id { get; set; }
            public string Email { get; set; }
        }

        private class StudentViewModel
        {
            public string StudentId { get; set; }
        }

        private class StudentViewModelTransformer : AbstractTransformerCreationTask<Student>
        {
            public StudentViewModelTransformer()
            {
                TransformResults = students => from student in students
                                               select new
                                               {
                                                   StudentId = student.Id.ToString().Split('/', StringSplitOptions.None)[1]
                                               };
            }
        }
    }
}
