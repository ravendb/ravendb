//-----------------------------------------------------------------------
// <copyright file="MetadataExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Database.Linq.PrivateExtensions
{
    public static class MetadataExtensions
    {
        public static IEnumerable<dynamic> WhereEntityIs(this IEnumerable<dynamic> self, params string[] metadata)
        {
            return self.Where(doc => metadata.Any(
                m => string.Equals(m, doc["@metadata"]["Raven-Entity-Name"], StringComparison.InvariantCultureIgnoreCase)));
        }
    }
}