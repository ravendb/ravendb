using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class LinqOnDictionary : RavenTest
	{
		[Fact]
		public void CanHandleQueriesOnDictionaries()
		{
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					//now the student:
					var student = new Student
					{
						Attributes = new Dictionary<string, string>
						{
							{"NIC", "studentsNICnumberGoesHere"}
						}
					};
					session.Store(student);
					session.SaveChanges();
				}


				//Testing query on attribute
				using (var session = store.OpenSession())
				{
					var result = from student in session.Query<Student>()
								 where student.Attributes["NIC"] == "studentsNICnumberGoesHere"
								 select student;

					var test = result.ToList();

					Assert.NotEmpty(test);
				}
			}
			
		}

		public class Student
		{
			public string Id { get; set; }
			public IDictionary<string, string> Attributes { get; set; }
		}
	}
}
