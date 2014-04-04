using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class When_trying_to_load_a_non_existing_document : RavenTestBase
    {
        [Fact]
        public async Task Then_null_should_be_returned()
        {
            var id = new Guid("803C807A-ADD5-49F2-A00A-E5891B343CF7");

            using (var store = NewDocumentStore())
            {
                new StudentViewModelTransformer().Execute(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(new Student { Id = id, Email = "support@hibernatingrhinos.com" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    string key = session.Advanced.DocumentStore.Conventions
                                        .FindFullDocumentKeyFromNonStringIdentifier(id, typeof(Student), false);

                    StudentViewModel studentVm = session.Load<StudentViewModelTransformer, StudentViewModel>(key);
                    Assert.NotNull(studentVm);

                    var nonExistingKey = session.Advanced.DocumentStore.Conventions
                                        .FindFullDocumentKeyFromNonStringIdentifier(Guid.NewGuid(), typeof(Student), false);

                    studentVm = session.Load<StudentViewModelTransformer, StudentViewModel>(nonExistingKey);
                    Assert.Null(studentVm);
                }

                using (var session = store.OpenAsyncSession())
                {
                    string key = session.Advanced.DocumentStore.Conventions
                                        .FindFullDocumentKeyFromNonStringIdentifier(id, typeof(Student), false);

                    StudentViewModel studentVm = await session.LoadAsync<StudentViewModelTransformer, StudentViewModel>(key);
                    Assert.NotNull(studentVm);

                    var nonExistingKey = session.Advanced.DocumentStore.Conventions
                                        .FindFullDocumentKeyFromNonStringIdentifier(Guid.NewGuid(), typeof(Student), false);

                    studentVm = await session.LoadAsync<StudentViewModelTransformer, StudentViewModel>(nonExistingKey);
                    Assert.Null(studentVm);
                }
            }
        }

        public class Student
        {
            public Guid Id { get; set; }
            public string Email { get; set; }
        }

        public class StudentViewModel
        {
            public Guid StudentId { get; set; }
        }

        public class StudentViewModelTransformer : AbstractTransformerCreationTask<Student>
        {
            public StudentViewModelTransformer()
            {
                TransformResults = students => from student in students
                                               select new
                                               {
                                                   StudentId = student.Id.ToString().Split('/')[1]
                                               };
            }
        }
    }
}