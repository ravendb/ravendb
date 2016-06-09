// -----------------------------------------------------------------------
//  <copyright file="SubscriptionCriteria.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
    public class SubscriptionCriteria
    {
        public string Collection { get; set; }
        public string FilterJavaScript { get; set; }
    }

    public class SubscriptionCriteria<T>
    {
        public string Collection { get; set; }
        public string FilterJavaScript { get; set; }
    }
}