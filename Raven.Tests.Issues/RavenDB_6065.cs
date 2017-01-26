using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class DocumentQueryWithDefaultOperator : RavenTest
    {
        public class Person
        {
            public string Name { get; set; }
            public char Gender { get; set; }
            public int Age { get; set; }
        }

        
        [Fact]
        public void QueryWithOrOperators()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "Fred", Gender = 'M', Age = 30 });
                    session.Store(new Person { Name = "John", Gender = 'M', Age = 23 });
                    session.Store(new Person { Name = "Sally", Gender = 'F', Age = 45 });
                    session.Store(new Person { Name = "Jane", Gender = 'F', Age = 16 });
                    session.Store(new Person { Name = "Matt", Gender = 'M', Age = 18 });
                    session.Store(new Person { Name = "Emma", Gender = 'F', Age = 28 });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<Person>()
                        .UsingDefaultOperator(QueryOperator.And) // Breaks the query, now only appears to match the last operand
                        .WhereEquals("Gender", 'F') // Should match 3 documents
                        .OrElse()
                        .WhereEquals("Name", "Fred") // Should match 1 document
                        .OrElse()
                        .WhereEquals("Age", 18); // Should match 1 document
                    var results = query.ToList();
                    Assert.Equal(5, results.Count);
                }
            }
        }
        
    }
}
