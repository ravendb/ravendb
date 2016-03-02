// -----------------------------------------------------------------------
//  <copyright file="DevelopmentHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Abstractions;

namespace Raven.Database.Util
{
    internal static class DevelopmentHelper
    {
        public static void TimeBomb()
        {
            if (SystemTime.UtcNow > new DateTime(2016, 4, 1))
                throw new NotImplementedException("Development time bomb.");
        }
    }
}
