// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1598_MappingComplexProperties.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;

    using Raven.Client.Indexes;
    using Raven.Tests.Bundles.ScriptedIndexResults;

    using Xunit;

    public class RavenDB_1598_MappingComplexProperties : RavenTestBase
    {
        public class ResultTypes
        {
            public string Name { get; set; }
            public string NameJson { get; set; }
            public string Animal { get; set; }
            public string AnimalJson { get; set; }
            public string StringArray_ZeroItems { get; set; }
            public string StringArray_ZeroItemsJson { get; set; }
            public string StringArray_OneItem { get; set; }
            public string StringArray_OneItemJson { get; set; }
            public string StringArray_OneItem_First { get; set; }
            public string StringArray_TwoItems { get; set; }
            public string StringArray_TwoItemsJson { get; set; }
            public string StringArray_TwoItems_First { get; set; }

            public string ObjectArray_ZeroItems { get; set; }
            public string ObjectArray_ZeroItemsJson { get; set; }
            public string ObjectArray_OneItem { get; set; }
            public string ObjectArray_OneItemJson { get; set; }
            public string ObjectArray_OneItem_First { get; set; }
            public string ObjectArray_TwoItems { get; set; }
            public string ObjectArray_TwoItemsJson { get; set; }
            public string ObjectArray_TwoItems_First { get; set; }
        }


        public class AnimalMapNonsense_Index : AbstractIndexCreationTask<Animal, AnimalMapNonsense_Index.Result>
        {
            public class Result
            {
                public string Name { get; set; }
                public Animal Animal { get; set; }
                public string[] StringArray_ZeroItems { get; set; }
                public string[] StringArray_OneItem { get; set; }
                public string[] StringArray_TwoItems { get; set; }

                public Animal[] ObjectArray_ZeroItems { get; set; }
                public Animal[] ObjectArray_OneItem { get; set; }
                public Animal[] ObjectArray_TwoItems { get; set; }
            }

            public AnimalMapNonsense_Index()
            {
                Map = animals =>
                      from animal in animals
                      select new Result
                      {
                          Name = animal.Name,
                          Animal = new Animal { Name = animal.Name, Type = animal.Type },
                          StringArray_ZeroItems = new string[0],
                          StringArray_OneItem = new[] { animal.Name },
                          StringArray_TwoItems = new[] { animal.Name, animal.Name },
                          ObjectArray_ZeroItems = new Animal[0],
                          ObjectArray_OneItem = new[] { new Animal { Name = animal.Name, Type = animal.Type } },
                          ObjectArray_TwoItems = new[] { new Animal { Name = animal.Name, Type = animal.Type }, new Animal { Name = animal.Name, Type = animal.Type } }
                      };

            }
        }

        public class AnimalReduceNonsense_Index : AnimalMapNonsense_Index
        {
            public AnimalReduceNonsense_Index()
            {
                Reduce = animals => animals.GroupBy(_ => _.Name).Select(_ => new Result
                {
                    Name = _.First().Name,
                    Animal = _.First().Animal,
                    StringArray_ZeroItems = _.First().StringArray_ZeroItems,
                    StringArray_OneItem = _.First().StringArray_OneItem,
                    StringArray_TwoItems = _.First().StringArray_TwoItems,
                    ObjectArray_ZeroItems = _.First().ObjectArray_ZeroItems,
                    ObjectArray_OneItem = _.First().ObjectArray_OneItem,
                    ObjectArray_TwoItems = _.First().ObjectArray_TwoItems,
                });

            }
        }

        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults";
        }

        [Fact]
        public void CheckTypesOnMap()
        {
            checkIndex(new AnimalMapNonsense_Index());
        }

        [Fact]
        public void CheckTypesOnReduce()
        {
            checkIndex(new AnimalReduceNonsense_Index());
        }

        private void checkIndex(AnimalMapNonsense_Index index)
        {
            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Raven.Abstractions.Data.ScriptedIndexResults
                    {
                        Id = Raven.Abstractions.Data.ScriptedIndexResults.IdPrefix + index.IndexName,
                        IndexScript = @"
var docId = 'ResultTypes/' + this.Name;
var doc = {
    Name: Object.prototype.toString.call(this.Name),
    NameJson: JSON.stringify(this.Name),
    Animal: Object.prototype.toString.call( this.Animal),
    AnimalJson: JSON.stringify( this.Animal),
    StringArray_ZeroItems: Object.prototype.toString.call( this.StringArray_ZeroItems),
    StringArray_ZeroItemsJson: JSON.stringify( this.StringArray_ZeroItems),
    StringArray_OneItem: Object.prototype.toString.call( this.StringArray_OneItem),
    StringArray_OneItemJson: JSON.stringify( this.StringArray_OneItem),
    StringArray_OneItem_First: Object.prototype.toString.call( this.StringArray_OneItem[0]),
    StringArray_TwoItems: Object.prototype.toString.call( this.StringArray_TwoItems),
    StringArray_TwoItemsJson: JSON.stringify( this.StringArray_TwoItems),
    StringArray_TwoItems_First: Object.prototype.toString.call( this.StringArray_TwoItems[0]),
    ObjectArray_ZeroItems: Object.prototype.toString.call( this.ObjectArray_ZeroItems),
    ObjectArray_ZeroItemsJson: JSON.stringify( this.ObjectArray_ZeroItems),
    ObjectArray_OneItem: Object.prototype.toString.call( this.ObjectArray_OneItem),
    ObjectArray_OneItemJson: JSON.stringify( this.ObjectArray_OneItem),
    ObjectArray_OneItem_First: Object.prototype.toString.call( this.ObjectArray_OneItem[0]),
    ObjectArray_TwoItems: Object.prototype.toString.call( this.ObjectArray_TwoItems),
    ObjectArray_TwoItemsJson: JSON.stringify( this.ObjectArray_TwoItems),
    ObjectArray_TwoItems_First: Object.prototype.toString.call( this.ObjectArray_TwoItems[0])
}
PutDocument(docId, doc);",
                        DeleteScript = @""
                    });
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    s.Store(new Animal
                    {
                        Name = "Arava",
                        Type = "Dog"
                    });

                    s.SaveChanges();
                }

                index.Execute(store);

                WaitForIndexing(store, timeout: TimeSpan.FromSeconds(360));

                using (var s = store.OpenSession())
                {
                    var actual = s.Load<ResultTypes>("ResultTypes/Arava");
                    Assert.NotNull(actual);

                    ResultTypes expected = new ResultTypes()
                    {
                        Name = "[object String]",
                        Animal = "[object Object]",
                        StringArray_ZeroItems = "[object Array]",
                        StringArray_OneItem = "[object Array]",
                        StringArray_OneItem_First = "[object String]",
                        StringArray_TwoItems = "[object Array]",
                        StringArray_TwoItems_First = "[object String]",

                        ObjectArray_ZeroItems = "[object Array]",
                        ObjectArray_OneItem = "[object Array]",
                        ObjectArray_OneItem_First = "[object Object]",
                        ObjectArray_TwoItems = "[object Array]",
                        ObjectArray_TwoItems_First = "[object Object]",

                        NameJson = @"""Arava""",
                        AnimalJson = @"{""Name"":""Arava"",""Type"":""Dog""}",
                        StringArray_ZeroItemsJson = @"[]",
                        StringArray_OneItemJson = @"[""Arava""]",
                        StringArray_TwoItemsJson = @"[""Arava"",""Arava""]",
                        ObjectArray_ZeroItemsJson = @"[]",
                        ObjectArray_OneItemJson = @"[{""Name"":""Arava"",""Type"":""Dog""}]",
                        ObjectArray_TwoItemsJson = @"[{""Name"":""Arava"",""Type"":""Dog""},{""Name"":""Arava"",""Type"":""Dog""}]",
                    };

                    Console.WriteLine("Replaced \\\" with ' for better readability...");
                    Console.WriteLine();
                    List<string> failed = new List<string>();
                    foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(typeof(ResultTypes)))
                    {
                        object actualP = property.GetValue(actual);

                        object expectedP = property.GetValue(expected);
                        var match = Equals(actualP, expectedP);
                        if (!match) failed.Add(property.Name);
                        if (expectedP is String) expectedP = ((string)expectedP).Replace("\\\"", "'");
                        if (actualP is String) actualP = ((string)actualP).Replace("\\\"", "'");
                        Console.WriteLine(property.Name.PadRight(30) + ": " + (match ? "OK   " : "FAIL ") + "expected " + expectedP + ", was " + actualP);
                    }
                    Assert.True(failed.Count == 0, "Properties with type missmatch: " + String.Join(", ", failed));
                }
            }
        }
    }
}