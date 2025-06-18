﻿using System.Collections.Generic;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_23848 : RavenTestBase
{
    public RavenDB_23848(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void should_generate_correct_rql_for_contains_all_with_empty_list()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var rql = session
                    .Advanced.DocumentQuery<Course>()
                    .WhereEquals(x => x.OwnerId, "test")
                    .ContainsAll(x => x.Tags, new List<string>())
                    .OpenSubclause()
                    .WhereEquals(x => x.Status, CourseStatus.Published)
                    .OrElse()
                    .WhereEquals(x => x.Status, CourseStatus.Draft)
                    .CloseSubclause()
                    .ToString();
                
                Assert.Equal("from 'Courses' where OwnerId = $p0 and true and (Status = $p1 or Status = $p2)", rql);
            }
        }
    }

    private class Course
    {
        public string OwnerId { get; set; }
        
        public string[] Tags { get; set; }
        
        public CourseStatus Status { get; set; }
    }
    
    private enum CourseStatus
    {
        Draft,
        Published
    }
}
