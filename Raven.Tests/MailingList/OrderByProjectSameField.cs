// -----------------------------------------------------------------------
//  <copyright file="OrderByProjectSameField.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Diagnostics;
using System.Linq;

using FizzWare.NBuilder;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class OrderByProjectSameField : RavenTestBase
    {

        public class CategoryIndex : AbstractMultiMapIndexCreationTask<CategoryIndexResult>
        {
            public CategoryIndex()
            {
                AddMap<Product>(docs => from doc in docs
                                        where !doc.Id.Contains("-")
                                        select new
                                        {
                                            CategoryId = doc.CategoryId,
                                            Name = (string)null,
                                            Count = 1
                                        });

                AddMap<Category>(docs => from doc in docs select new { CategoryId = doc.Id, Name = doc.Name, Count = 0 });

                Reduce = results => from result in results
                                    group result by result.CategoryId
                                        into g
                                        select new CategoryIndexResult
                                        {
                                            CategoryId = g.Key,
                                            Name = g.Select(x => x.Name).DefaultIfEmpty("Unassigned Products").FirstOrDefault(x => x != null),
                                            Count = g.Sum(x => x.Count)
                                        };

                Store(x=>x.Name, FieldStorage.Yes);
            }
        }

        public class CategoryIndexResult
        {
            public string CategoryId { get; set; }

            public string Name { get; set; }

            public int Count { get; set; }
        }

        public class Category
        {
            public Category()
            {
                CustomerEnabled = true;
            }

            #region Properties

            public string Id { get; set; }

            /// <summary>
            /// Gets or sets the description.
            /// </summary>
            /// <value>The description.</value>
            public string Description { get; set; }

            /// <summary>
            /// Gets or sets a value indicating whether this <see cref="ICategory"/> is enabled.
            /// </summary>
            /// <value><c>true</c> if enabled; otherwise, <c>false</c>.</value>
            public bool CustomerEnabled { get; set; }

            /// <summary>
            /// Gets or sets the name.
            /// </summary>
            /// <value>The  name.</value>
            public string Name { get; set; }

            /// <summary>
            /// Gets or sets the ordinal.
            /// </summary>
            /// <value>The ordinal.</value>
            public int Ordinal { get; set; }

            /// <summary>
            /// Gets or sets the parent id.
            /// </summary>
            /// <value>The parent id.</value>
            public int? ParentId { get; set; }

            /// <summary>
            /// Gets or sets the product count.
            /// </summary>
            /// <value>The product count.</value>
            public int ProductCount { get; set; }

            /// <summary>
            /// Gets or sets the products outstanding.
            /// </summary>
            /// <value>The products outstanding (used in the edit db only).</value>
            public int ProductsOutstanding { get; set; }

            /// <summary>
            /// Gets or sets the unspsc.
            /// </summary>
            /// <value>The unspsc.</value>
            public string Unspsc { get; set; }

            #endregion
        }

        [DebuggerDisplay("Id: {Id} Code: {Code} CategoryId: {CategoryId} ")]
        public class Product
        {
            private string id;

            private string rsId = ConfigurationManager.AppSettings["RSComp1ProductId"];

            private string groupId;

            private string categoryId;

            private IList<string> imagesId = new List<string>();

            public Product()
            {
                AssignedTags = new List<string>();
                Categories = new string[0];
                imagesId = new List<string>();
            }

            public string Id
            {
                get
                {
                    return id;
                }

                set
                {
                    id = value;
                }
            }

            public bool CustomerEnabled { get; set; }

            public string Description { get; set; }

            public string CategoryId
            {
                get
                {
                    return categoryId;
                }

                set
                {
                    categoryId = value;
                    GroupId = null;
                }
            }

            [Description("Generated by the system, this contains the entire category breadcrumb back to the root")]
            public string[] Categories { get; set; }

            public bool Component { get; set; }

            public bool Dangerous { get; set; }

            public bool Discontinued { get; set; }

            public bool Hazardous { get; set; }

            public bool HazardousArea { get; set; }

            public double LeadTime { get; set; }

            [Obsolete("Manufacturer object now in product, Id has been deprecated, use code's instead", true)]
            public string ManufacturerId { get; set; }

            public string Code { get; set; }

            public string Name { get; set; }

            public bool? RoHS { get; set; }

            public string SupplierCode { get; set; }

            public string UNSPSC { get; set; }

            public double Weight { get; set; }

            public bool Inactive { get; set; }

            public string GroupId
            {
                get
                {
                    return groupId;
                }

                set
                {
                    groupId = value;
                    this.categoryId = null;
                }
            }

            public IList<string> AssignedTags { get; set; }

            public string[] Images
            {
                get
                {
                    return imagesId.ToArray();
                }

                private set
                {
                    imagesId = value.ToList();
                }
            }

            public void AddImage(string imageId)
            {
                if (!imagesId.Contains(imageId))
                    imagesId.Add(imageId);
            }

            public void RemoveImage(string imageId)
            {
                imagesId.Remove(imageId);
            }

            public void ClearImages()
            {
                imagesId.Clear();
            }
        }
        public List<Category> Categories { get; set; }

        public List<Product> Products { get; set; }

        
        [Fact]
        public void FindCategoryByName_Works()
        {

            using (var store = NewDocumentStore())
            {
                new CategoryIndex().Execute(store);


                this.Categories =
                    (List<Category>) Builder<Category>.CreateListOfSize(5).All().With(x => x.Id, null).Build();

                this.Products = (List<Product>) Builder<Product>.CreateListOfSize(5).All().With(x => x.Id, null).Build();

                using (var session = store.OpenSession())
                {
                    foreach (var category in Categories)
                    {
                        session.Store(category);
                    }
                    foreach (var product in Products)
                    {
                        session.Store(product);
                    }

                    session.SaveChanges();
                }

                var expected = Categories.First().Name;

                using (var session = store.OpenSession())
                {
                    var actual = CategoryNotWorking(session, expected);
                    Assert.Single(actual, x => x == expected);
                }
            }
        }

        public string[] CategoryNotWorking(IDocumentSession session, string text, int count = 8)
        {
            text = TextWildcard(text);

            var query =
                session.Query<Category, CategoryIndex>()
                       .Customize(x => x.WaitForNonStaleResults())
                       .Search(x => x.Name, text, 1, SearchOptions.Or, EscapeQueryOptions.AllowPostfixWildcard)
                       .OrderBy(x => x.Name)
                       .Select(x => x.Name);

            var results = query.ToList();

            var category = query.ToArray();
            return category.Distinct().Take(count).ToArray();
        }

        public string[] CategoryWorking(IDocumentSession session, string text, int count = 8)
        {
            text = TextWildcard(text);

            var query =
                session.Query<Category, CategoryIndex>()
                       .Customize(x => x.WaitForNonStaleResults())
                       .Search(x => x.Name, text, 1, SearchOptions.Or, EscapeQueryOptions.AllowPostfixWildcard)
                       .OrderBy(x => x.Name);

            var results = query.ToList();

            var category = query.ToArray();
            return category.Select(x => x.Name).Distinct().Take(count).ToArray();
        }

        private static string TextWildcard(string text)
        {
            return text.EndsWith("*") ? text : text + "*";
        }
    }

}