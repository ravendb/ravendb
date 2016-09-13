//-----------------------------------------------------------------------
// <copyright file="LinqOnDynamic.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Linq;

namespace Raven.Server.Documents.Indexes.Static.Extensions
{
    public static class LinqOnDynamic
    {
        public static IEnumerable<IGrouping<dynamic, dynamic>> GroupBy(this IEnumerable<dynamic> source, Func<dynamic, dynamic> keySelector)
        {
            return Enumerable.GroupBy(source, keySelector).Select(inner => new DynamicArray.DynamicGrouping(inner));
        }

        public static IEnumerable<IGrouping<dynamic, dynamic>> GroupBy(this IEnumerable<dynamic> source, Func<dynamic, dynamic> keySelector, Func<dynamic, dynamic> resultSelector)
        {
            return Enumerable.GroupBy(source, keySelector, resultSelector).Select(inner => new DynamicArray.DynamicGrouping(inner));
        }

        public static IEnumerable<dynamic> DefaultIfEmpty(this IEnumerable<dynamic> self)
        {
            return self.DefaultIfEmpty<dynamic>(DynamicNullObject.Null);
        }
    }
}
