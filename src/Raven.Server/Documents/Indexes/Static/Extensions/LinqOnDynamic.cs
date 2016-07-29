//-----------------------------------------------------------------------
// <copyright file="LinqOnDynamic.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Linq;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static.Extensions
{
    public static class LinqOnDynamic
    {
        public static IEnumerable<dynamic> DefaultIfEmpty(this IEnumerable<dynamic> self)
        {
            return self.DefaultIfEmpty<dynamic>(DynamicNullObject.Null);
        }
    }
}
