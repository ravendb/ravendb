using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Database.Linq.PrivateExtensions;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class JBA : RavenTest
	{
		[Fact]
		public void Can_define_index_with_WhereEntityIs()
		{
			var idxBuilder = new IndexDefinitionBuilder<object>
			{
				Map =
					docs =>
					from course in (IEnumerable<Course>) docs
					select new {course.Id, course},

				TransformResults =
					(database, docs) =>
					from course in (IEnumerable<Course>) docs
					select new
					{
						item = course.Name,
						id = course.Id,
						iconCls = "course",
						leaf = false,
						expanded = true,
						children =
						from unit in course.Syllabus
						select new
						{
							item = unit.Name,
							id = unit.Name,
							iconCls = "unit",
							leaf = false,
							expanded = true,
							children =
							from notebook in unit.Notebooks
							select new
							{
								item = notebook.Name,
								id = notebook.Id,
								courseId = course.Id,
								iconCls = "notebook",
								type = notebook.Type,
								leaf = true,
							}
						}
					}
			};

			using(var store = NewDocumentStore())
			{
				var indexDefinition = idxBuilder.ToIndexDefinition(store.Conventions);
				store.DatabaseCommands.PutIndex("test", indexDefinition);
			}
		}

		public class Course
		{
			public string Id { get; set; }

			public string Name { get; set; }

			public IEnumerable<Class> Syllabus { get; set; }
		}



		public class Class
		{
			public string Name { get; set; }

			public IEnumerable<Notebook> Notebooks { get; set; }

			
		}

		public class Notebook
		{
			public string Id { get; set; }

			public string Name { get; set; }

			public string Type { get; set; }
		}
	}
}