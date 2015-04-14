// -----------------------------------------------------------------------
//  <copyright file="NestedPropertiesIndex_1182.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Abstractions.Indexing;
	using Raven.Client.Indexes;

	using Xunit;

	public class NestedPropertiesIndex_1182 : RavenTest
	{
		private class Employee
		{
			public string Name { get; set; }

			public Address Address { get; set; }

			public IList<Skill> Skills { get; set; }
		}

		private class Skill
		{
			public IList<string> Projects { get; set; }
		}

		private class Address
		{
			public IList<string> HouseholdMembers { get; set; }
		}

		private class Employee_Test_Index : AbstractIndexCreationTask<Employee>
		{
			public Employee_Test_Index()
			{
				Map = employees => from employee in employees
								   from address in employee.Address.HouseholdMembers.DefaultIfEmpty()
								   from skill in employee.Skills.DefaultIfEmpty()
								   from project in skill.Projects.DefaultIfEmpty()
								   select new
										  {
											  Name = employee.Name
										  };
			}
		}

		[Fact]
		public void ShouldWork1()
		{
			Assert.DoesNotThrow(
				() =>
				{
					using (var store = NewDocumentStore())
					{
						store.DatabaseCommands.PutIndex("EmployeeTestIndex", new IndexDefinition
						{
							Map = @"from doc in docs.Employees
                                    from doc_Address in doc.Address.HouseholdMembers.DefaultIfEmpty()
                                    from doc_Skills in doc.Skills
                                    from doc_Skills_Projects in doc_Skills.Projects.DefaultIfEmpty()
                                    select new
                                    {
                                        Name = doc.Name
                                    }"
						});
					}
				});
		}

		[Fact]
		public void ShouldWork2()
		{
			Assert.DoesNotThrow(
				() =>
				{
					using (var store = NewDocumentStore())
					{
						new Employee_Test_Index().Execute(store);
					}
				});
		}

		[Fact]
		public void ShouldWork3()
		{
			using (var store = NewDocumentStore())
			{
				new Employee_Test_Index().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Employee
								  {
									  Name = "Employee1",
									  Address = new Address
												{
													HouseholdMembers = new List<string>
													                   {
														                   "John",
																		   "Edward"
													                   }
												},
									  Skills = new[]
									           {
										           new Skill
										           {
											           Projects = new List<string> { "Project1", "Project2" }
										           },
												   new Skill
										           {
											           Projects = new List<string> { "Project3", "Project4" }
										           },
									           }
								  });

					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var result = session
						.Query<Employee, Employee_Test_Index>()
						.ToList();

					Assert.Equal(1, result.Count);
				}
			}
		}
	}
}