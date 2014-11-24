// -----------------------------------------------------------------------
//  <copyright file="SizeHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Util
{
    public static class SizeHelper
    {
        public static string Humane(long? size)
        {
            if (size == null)
                return null;

            var absSize = Math.Abs(size.Value);
            const double GB = 1024 * 1024 * 1024;
            const double MB = 1024 * 1024;
            const double KB = 1024;

            if (absSize > GB) // GB
                return string.Format("{0:#,#.##;;0} GBytes", size / GB);
            if (absSize > MB)
                return string.Format("{0:#,#.##;;0} MBytes", size / MB);
            if (absSize > KB)
                return string.Format("{0:#,#.##;;0} KBytes", size / KB);
            return string.Format("{0:#,#;;0} Bytes", size);
        }
    }
}