using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Util;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1539 : RavenTestBase
	{
		public class TestDoc
		{
			public string Id { get; set; }
			public string Data { get; set; }
		}

		[Fact]
		public void Several_SaveChanges_for_the_same_document_in_single_transaction_and_the_same_session_should_work()
		{
            using (var documentStore = NewRemoteDocumentStore(runInMemory: false, requestedStorage: "esent"))
			using (var session = documentStore.OpenSession())
			{
                if(documentStore.DatabaseCommands.GetStatistics().SupportsDtc == false)
                    return;

				session.Advanced.UseOptimisticConcurrency = true;
				session.Advanced.AllowNonAuthoritativeInformation = false;

				using (var transaction = new TransactionScope())
				{
					var newDoc = new TestDoc { Data = "Foo" };
					session.Store(newDoc);
					session.SaveChanges();

					newDoc.Data = "Bar";
					session.SaveChanges();

					newDoc.Data = "Foo-Bar!";
					Assert.DoesNotThrow(session.SaveChanges); //should not throw concurrency exception

					transaction.Complete();
				}
			}
		}

		[Fact]
		public void Several_SaveChanges_for_the_same_document_in_single_transaction_should_allow_commit_without_concurrency_exception()
		{
            using (var documentStore = NewRemoteDocumentStore(runInMemory: false, requestedStorage: "esent"))
			using (var session = documentStore.OpenSession())
			{
                if (documentStore.DatabaseCommands.GetStatistics().SupportsDtc == false)
                    return;

				session.Advanced.UseOptimisticConcurrency = true;
				session.Advanced.AllowNonAuthoritativeInformation = false;

				Assert.DoesNotThrow(() =>
				{
					using (var transaction = new TransactionScope())
					{
						var newDoc = new TestDoc { Data = "Foo" };
						session.Store(newDoc);
						session.SaveChanges();

						newDoc.Data = "Bar";
						session.SaveChanges();

						transaction.Complete();
					}
				});
			}
		}


		[Fact]
		public void StoreAndSaveThenUpdateNewDocumentInsideTransactionSucceedsUsingSession()
		{
			var input = GenerateEditable();

			using (var documentStore = NewRemoteDocumentStore(runInMemory: false, requestedStorage: "voron"))
			using (var session = documentStore.OpenSession())
			{
                if (documentStore.DatabaseCommands.GetStatistics().SupportsDtc == false)
                    return;

				session.Advanced.UseOptimisticConcurrency = true;
				session.Advanced.AllowNonAuthoritativeInformation = false;

				using (var transaction = new TransactionScope())
				{
					var studentIds = new List<string>();
					var students = new List<Student>();

					input.Students.ForEach(x =>
					{
						var updatedStudent = new Student
						{
							Name = x.Name,
							Email = x.Email,
							Id = BaseIdentity<Student>.IdTemplate()
						};

						session.Store(updatedStudent);
						session.SaveChanges();

						updatedStudent.LastUpdatedBy = "bob";

						studentIds.Add(updatedStudent.Id);
						students.Add(updatedStudent);
					});

					var updatedCourse = new Course
					{
						Name = input.Course.Name,
						Students = studentIds,
						LastUpdatedBy = "bob"
					};

					session.Store(updatedCourse);
					session.SaveChanges();

					students.ForEach(x =>
					{
						x.CourseId = updatedCourse.Id;
					});

					Assert.DoesNotThrow(session.SaveChanges);

					transaction.Complete();
				}
			}
		}

		private static Editable GenerateEditable()
		{
			return new Editable()
			{
				Course = new EditableCourse
				{
					Name = "Biology 101",
				},
				Students = new List<EditableStudent>
                {
                    new EditableStudent
                    {
                        Name = "Bob Builder1",
                        Email = "support@hibernatingrhinos1.com"
                    },
                    new EditableStudent
                    {
                        Name = "Bob Builder2",
                        Email = "support@hibernatingrhinos2.com"
                    },
                }
			};
		}

		public interface IBase
		{
			bool Deleted { get; set; }
			DateTime LastUpdated { get; set; }
			string LastUpdatedBy { get; set; }
		}

		public interface IBase<T> : IBase
		{
			T Id { get; set; }
		}

		public interface IBaseIdentity : IBase<string>
		{
		}

		public abstract class Base<T>
		{
			public T Id { get; set; }
		}

		public abstract class BaseIdentity<T> : Base<string>, IBaseIdentity
			where T : class
		{
			private static readonly string Name = Inflector.Pluralize(typeof(T).Name).ToLower();

			protected BaseIdentity()
			{
				Id = string.Format("{0}/", Name);
			}

			public DateTime LastUpdated { get; set; }
			public string LastUpdatedBy { get; set; }

			public static string AsId(int index)
			{
				return string.Format("{0}/{1}", Name, index);
			}

			public static string IdTemplate()
			{
				return string.Format("{0}/", Name);
			}

			public bool Deleted { get; set; }
		}

		public class EditableStudent
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
		}

		public class EditableCourse
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Editable
		{
			public EditableCourse Course { get; set; }
			public List<EditableStudent> Students { get; set; }
		}

		public class Student : BaseIdentity<Student>
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
			public string CourseId { get; set; }
		}

		public class Course : BaseIdentity<Course>
		{
			public string Id { get; set; }
			public string Name { get; set; }

			public List<string> Students { get; set; }
		}

		public class Meal : BaseIdentity<Meal>
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}
