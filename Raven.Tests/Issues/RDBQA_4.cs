// -----------------------------------------------------------------------
//  <copyright file="RDBQA_4.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RDBQA_4 : RavenTest
    {
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

            }
        }
       
    }

}