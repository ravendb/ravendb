// -----------------------------------------------------------------------
//  <copyright file="RDBQA_4.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RDBQA_4 : RavenTest
    {
        const string groupsString = @"{
                                     
                    'Groups': [
                    {
                        'Name': 'g1',
                        'Members': [
                            { 'Name': 'John', 'Nick': 'yes60' },
                            { 'Name': 'Smith', 'Nick': 'blacksmith' }
                        ]
                    },
                    {
                        'Name': 'g2',
                        'Members': [
                            { 'Name': 'Amy', 'Nick': 'amy1988' },
                            { 'Name': 'Adele', 'Nick': 'music02' }
                        ]
                    }]
                    }
                    ";

        [Fact]
        public void Test()
        {
            using (var store = NewDocumentStore())
            {
                const string objectString = @"{
                    'Company': 'companies/85',
                    'Employee': 'employees/5',
                    'OrderedAt': '1996-07-04T00:00:00.0000000',
                    'RequireAt': '1996-08-01T00:00:00.0000000',
                    'ShippedAt': '1996-07-16T00:00:00.0000000',
                    'ShipTo': {
                    'Line1': '59 rue de l\'Abbaye',
                    'Line2': null,
                    'City': 'Reims',
                    'Region': null,
                    'PostalCode': '51100',
                    'Country': 'France'
                    },
                    'ShipVia': 'shippers/3',
                    'Freight': 32.38,
                    'Lines': [
                    {
                    'Product': 'products/11',
                    'ProductName': 'Queso Cabrales',
                    'PricePerUnit': 14.0,
                    'Quantity': 12,
                    'Discount': 0.0
                    },
                    {
                    'Product': 'products/42',
                    'ProductName': 'Singaporean Hokkien Fried Mee',
                    'PricePerUnit': 9.8,
                    'Quantity': 10,
                    'Discount': 0.0
                    },
                    {
                    'Product': 'products/72',
                    'ProductName': 'Mozzarella di Giovanni',
                    'PricePerUnit': 34.8,
                    'Quantity': 5,
                    'Discount': 0.0
                    }
                    ]
                    }";

                store.DatabaseCommands.Put("orders/1", null, RavenJObject.Parse(objectString), new RavenJObject());

                store.DatabaseCommands.Patch("orders/1", new []
                {
                    new PatchRequest
                    {
                        Type = PatchCommandType.Insert,
                        Name = "Lines",
                        Position = 1,
                        Value = RavenJObject.Parse("{a:1}")
                    }
                });

                Assert.Equal(4, store.DatabaseCommands.Get("orders/1").DataAsJson.Value<RavenJArray>("Lines").Length);

            }
        }

        [Fact]
        public void TestInsertNested()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("groups/1", null, RavenJObject.Parse(groupsString), new RavenJObject());
                
                store.DatabaseCommands.Patch("groups/1", new[]
                {
                    new PatchRequest
                    {
                        Type = PatchCommandType.Modify,
                        Name = "Groups",
                        Position = 1,
                        Nested = new[]
                        {
                            new PatchRequest
                            {
                                Type = PatchCommandType.Insert,
                                Name = "Members",
                                Value = RavenJToken.Parse("{  'Name' : 'Pinky', 'Nick': 'pinky' }")
                            }
                        }
                    }
                });

                var groups = store.DatabaseCommands.Get("groups/1").DataAsJson;
                var groupsInner = groups.Value<RavenJArray>("Groups");
                Assert.Equal(2, groupsInner.Length);
                var members = groupsInner[1].Value<RavenJArray>("Members");
                Assert.Equal(3, members.Length);
                Assert.Equal("Pinky", members[2].Value<string>("Name"));

            }
        }

        [Fact]
        public void TestRemoveNested()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("groups/1", null, RavenJObject.Parse(groupsString), new RavenJObject());

                store.DatabaseCommands.Patch("groups/1", new[]
                {
                    new PatchRequest
                    {
                        Type = PatchCommandType.Modify,
                        Name = "Groups",
                        Position = 1,
                        Nested = new[]
                        {
                            new PatchRequest
                            {
                                Type = PatchCommandType.Remove,
                                Position = 0,
                                Name = "Members",
                            }
                        }
                    }
                });

                var groups = store.DatabaseCommands.Get("groups/1").DataAsJson;
                var groupsInner = groups.Value<RavenJArray>("Groups");
                Assert.Equal(2, groupsInner.Length);
                var members = groupsInner[1].Value<RavenJArray>("Members");
                Assert.Equal(1, members.Length);
                Assert.Equal("Adele", members[0].Value<string>("Name"));

            }
        }
       
    }

}