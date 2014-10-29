// -----------------------------------------------------------------------
//  <copyright file="IncludeOnDictionary.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Raven.Tests.Common;

using Xunit;
using System.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Tests.MailingList
{
    public class IncludeOnDictionary : NoDisposalNeeded
    {

        public class Basket
        {
            public Dictionary<string, Dictionary<string, Item>> Items { get; set; }
        }

        public class Item
        {
            public Dictionary<string, Product> Products { get; set; }
        }
        public class Product
        {
            public string IdRef { get; set; }
        }


        [Fact]
        public void UsingValues()
        {
            Expression<Func<Item, object>> expr = item => item.Products.Values.Select(x => x.IdRef);
            Assert.Equal("Products,IdRef", expr.ToPropertyPath());
        }

        [Fact]
        public void UsingValue()
        {
            Expression<Func<Item, object>> expr = item => item.Products.Select(x => x.Value.IdRef);
            Assert.Equal("Products,IdRef", expr.ToPropertyPath());
        }
    }

}