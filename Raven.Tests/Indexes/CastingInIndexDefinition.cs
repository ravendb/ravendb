using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class CastingInIndexDefinition : RavenTest
	{
		[Fact]
		public void CanCastValuesToString()
		{
			using (var documentStore = this.GetDocumentStore())
			{
				documentStore.ExecuteIndex(new Employees_CurrentCount());

				// Store some documents
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Employee { Id = "employees/1", Name = "John", PayRate = 10 });
					session.Store(new Employee { Id = "employees/2", Name = "Mary", PayRate = 20 });
					session.Store(new Employee { Id = "employees/3", Name = "Sam", PayRate = 30 });

					session.SaveChanges();
				}

				// Make some changes
				using (var session = documentStore.OpenSession())
				{
					var employee1 = session.Load<Employee>("employees/1");
					var metadata1 = session.Advanced.GetMetadataFor(employee1);
					metadata1["test"] = "1";

					var employee2 = session.Load<Employee>("employees/2");
					var metadata2 = session.Advanced.GetMetadataFor(employee2);
					metadata2["test"] = "2";

					var employee3 = session.Load<Employee>("employees/3");
					var metadata3 = session.Advanced.GetMetadataFor(employee3);
					metadata3["test"] = "2";

					session.SaveChanges();
				}

				// Query and check the results
				using (var session = documentStore.OpenSession())
				{
					var result = session.Query<Employees_CurrentCount.Result, Employees_CurrentCount>()
										.Customize(x => x.WaitForNonStaleResults())
										.ToList();
					WaitForUserToContinueTheTest(documentStore);
					Assert.Equal(2, result.FirstOrDefault().Count);
				}

			}
		}

		private EmbeddableDocumentStore GetDocumentStore()
		{
			var documentStore = new EmbeddableDocumentStore { RunInMemory = true };
			documentStore.Initialize();

			return documentStore;
		}

		public class Employees_CurrentCount : AbstractIndexCreationTask<Employee, Employees_CurrentCount.Result>
		{
			public class Result
			{
				public int Count { get; set; }
			}

			public Employees_CurrentCount()
			{
				Map = employees => from employee in employees
								   let status = MetadataFor(employee)
								   where status.Value<string>("test") == "2"
								   select new
								   {
									   Count = 1
								   };

				Reduce = results => from result in results
									group result by 0
										into g
										select new
										{
											Count = g.Sum(x => x.Count)
										};
			}
		}

		public class Employee
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public decimal PayRate { get; set; }
		}
	}
}
