// -----------------------------------------------------------------------
//  <copyright file="IndexPrettyPrinterEqualityComparer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.Util
{
    public class IndexPrettyPrinterEqualityComparer : IEqualityComparer<string>
    {
        public bool Equals(string x, string y)
        {
            if (x == y)
                return true;
            if (x == null || y == null)
                return false;

            var xFormatted = IndexPrettyPrinter.TryFormat(x);
            var yFormatted = IndexPrettyPrinter.TryFormat(y);
            return xFormatted.Equals(yFormatted);
        }

        public int GetHashCode(string obj)
        {
            throw new NotSupportedException();
        }
    }
}
