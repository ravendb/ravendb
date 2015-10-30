// -----------------------------------------------------------------------
//  <copyright file="ReferencedDocuments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using Xunit;
using Raven.Tests.Core.Utils.Transformers;
using System.Collections.Generic;

namespace Raven.Tests.Core.Indexing
{
    public class ReferencedDocuments : RavenCoreTestBase
    {
        [Fact]
        public void CanUseLoadDocumentToIndexReferencedDocs()
        {
            using (var store = GetDocumentStore())
            {
                var postsByContent = new Posts_ByContent();
                postsByContent.Execute(store);

                var companiesWithEmployees = new Companies_WithReferencedEmployees();
                companiesWithEmployees.Execute(store);

                var companiesWithEmployeesTransformer = new CompanyEmployeesTransformer();
                companiesWithEmployeesTransformer.Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Post
                        {
                            Id = "posts/" + i
                        });

                        session.Store(new PostContent
                        {
                            Id = "posts/" + i + "/content",
                            Text = i % 2 == 0 ? "HTML 5" : "Javascript"
                        });

                        session.Store(new Employee
                        {
                            Id = "employees/" + i,
                            LastName = "Last Name " + i
                        });
                    }

                    session.Store(new Company { EmployeesIds = new List<string>() { "employees/1", "employees/2", "employees/3" } });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var html5PostsQuery = session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "HTML 5");
                    var javascriptPostsQuery = session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "Javascript");

                    Assert.Equal(5, html5PostsQuery.ToList().Count);
                    Assert.Equal(5, javascriptPostsQuery.ToList().Count);


                    var companies = session.Advanced.DocumentQuery<Companies_WithReferencedEmployees.CompanyEmployees>(companiesWithEmployees.IndexName)
                        .SetResultTransformer(companiesWithEmployeesTransformer.TransformerName)
                        .ToArray();

                    Assert.Equal(1, companies.Length);
                    Assert.Equal("Last Name 1", companies[0].Employees[0]);
                    Assert.Equal("Last Name 2", companies[0].Employees[1]);
                    Assert.Equal("Last Name 3", companies[0].Employees[2]);
                }
            }
        }

        [Fact]
        public void ShouldReindexOnReferencedDocumentChange()
        {
            using (var store = GetDocumentStore())
            {
                var postsByContent = new Posts_ByContent();
                postsByContent.Execute(store);

                using (var session = store.OpenSession())
                {
                    PostContent last = null;
                    for (int i = 0; i < 3; i++)
                    {
                        session.Store(new Post
                        {
                            Id = "posts/" + i
                        });

                        session.Store(last = new PostContent
                        {
                            Id = "posts/" + i + "/content",
                            Text = i % 2 == 0 ? "HTML 5" : "Javascript"
                        });
                    }

                    session.SaveChanges();
                    WaitForIndexing(store);

                    Assert.Equal(2, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "HTML 5").ToList().Count);
                    Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "Javascript").ToList().Count);

                    last.Text = "JSON"; // referenced document change

                    session.Store(last);

                    session.SaveChanges();
                    WaitForIndexing(store);

                    Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "HTML 5").ToList().Count);
                    Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "Javascript").ToList().Count);
                    Assert.Equal(1, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "JSON").ToList().Count);

                    session.Delete(last); // referenced document delete

                    session.SaveChanges();
                    WaitForIndexing(store);

                    Assert.Equal(0, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", "JSON").ToList().Count);
                }
            }
        }

        [Fact]
        public void CanProceedWhenReferencedDocumentsAreMissing()
        {
            using (var store = GetDocumentStore())
            {
                var postsByContent = new Posts_ByContent();
                postsByContent.Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Post
                        {
                            Id = "posts/" + i
                        });

                        if (i % 2 == 0)
                        {
                            session.Store(new PostContent
                            {
                                Id = "posts/" + i + "/content",
                                Text = "HTML 5"
                            });
                        }
                    }

                    session.SaveChanges();
                    WaitForIndexing(store);

                    Assert.Equal(5, session.Advanced.DocumentQuery<Post>(postsByContent.IndexName).WhereEquals("Text", null).ToList().Count);
                }
            }
        }
    }
}
