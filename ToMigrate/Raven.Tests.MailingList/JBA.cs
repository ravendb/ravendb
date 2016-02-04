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
            var idxBuilder = new IndexDefinitionBuilder<object>("test")
            {
                Map =
                    docs =>
                    from course in (IEnumerable<Course>) docs
                    select new {course.Id, course},
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
