// -----------------------------------------------------------------------
//  <copyright file="RavenDB_XXXX.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Database.Indexing.IndexMerging;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4099 : RavenTest
    {
        private class Person_ByName_1 : AbstractIndexCreationTask<Person>
        {
            public Person_ByName_1()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     Name = p.Name
                                 };
            }
        }

        private class Person_ByName_2 : AbstractIndexCreationTask<Person>
        {
            public Person_ByName_2()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     Name = p.Name
                                 };
            }
        }

        private class Person_ByName_3 : AbstractIndexCreationTask<Person>
        {
            public Person_ByName_3()
            {
                Map = persons => from person in persons
                                 select new
                                 {
                                     Name = person.Name
                                 };
            }
        }

        private class Complex_Person_ByName_1 : AbstractIndexCreationTask<PersonWithAddress>
        {
            public Complex_Person_ByName_1()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     Street = p.Address.Street
                                 };
            }
        }

        private class Complex_Person_ByName_2 : AbstractIndexCreationTask<PersonWithAddress>
        {
            public Complex_Person_ByName_2()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     Street = p.Address.Street
                                 };
            }
        }

        private class Complex_Person_ByName_3 : AbstractIndexCreationTask<PersonWithAddress>
        {
            public Complex_Person_ByName_3()
            {
                Map = persons => from person in persons
                                 select new
                                 {
                                     Street = person.Address.Street
                                 };
            }
        }

        [Fact]
        public void IndexMergerShouldNotTakeIntoAccountExpressionVariableName()
        {
            var index1 = new Person_ByName_1();
            var index2 = new Person_ByName_2();
            var index3 = new Person_ByName_3();

            var indexDefinition1 = index1.CreateIndexDefinition();
            indexDefinition1.IndexId = 1;
            indexDefinition1.Name = index1.IndexName;

            var indexDefinition2 = index2.CreateIndexDefinition();
            indexDefinition1.IndexId = 2;
            indexDefinition2.Name = index2.IndexName;

            var indexDefinition3 = index3.CreateIndexDefinition();
            indexDefinition1.IndexId = 3;
            indexDefinition3.Name = index3.IndexName;

            var merger = new IndexMerger(
                new Dictionary<int, IndexDefinition>
                {
                    { indexDefinition1.IndexId, indexDefinition1 },
                    { indexDefinition2.IndexId, indexDefinition2 }
                });

            var results = merger.ProposeIndexMergeSuggestions();

            Assert.Equal(1, results.Suggestions.Count);
            Assert.Equal(1, results.Suggestions[0].CanDelete.Count);

            merger = new IndexMerger(
                new Dictionary<int, IndexDefinition>
                {
                    { indexDefinition1.IndexId, indexDefinition1 },
                    { indexDefinition3.IndexId, indexDefinition3 }
                });

            results = merger.ProposeIndexMergeSuggestions();

            Assert.Equal(1, results.Suggestions.Count);
            Assert.Equal(1, results.Suggestions[0].CanDelete.Count);
        }

        [Fact]
        public void IndexMergerShouldNotTakeIntoAccountExpressionVariableNameForComplexTypes()
        {
            var index1 = new Complex_Person_ByName_1();
            var index2 = new Complex_Person_ByName_2();
            var index3 = new Complex_Person_ByName_3();

            var indexDefinition1 = index1.CreateIndexDefinition();
            indexDefinition1.IndexId = 1;
            indexDefinition1.Name = index1.IndexName;

            var indexDefinition2 = index2.CreateIndexDefinition();
            indexDefinition1.IndexId = 2;
            indexDefinition2.Name = index2.IndexName;

            var indexDefinition3 = index3.CreateIndexDefinition();
            indexDefinition1.IndexId = 3;
            indexDefinition3.Name = index3.IndexName;

            var merger = new IndexMerger(
                new Dictionary<int, IndexDefinition>
                {
                    { indexDefinition1.IndexId, indexDefinition1 },
                    { indexDefinition2.IndexId, indexDefinition2 }
                });

            var results = merger.ProposeIndexMergeSuggestions();

            Assert.Equal(1, results.Suggestions.Count);
            Assert.Equal(1, results.Suggestions[0].CanDelete.Count);

            merger = new IndexMerger(
                new Dictionary<int, IndexDefinition>
                {
                    { indexDefinition1.IndexId, indexDefinition1 },
                    { indexDefinition3.IndexId, indexDefinition3 }
                });

            results = merger.ProposeIndexMergeSuggestions();

            Assert.Equal(1, results.Suggestions.Count);
            Assert.Equal(1, results.Suggestions[0].CanDelete.Count);
        }
    }
}