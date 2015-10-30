// -----------------------------------------------------------------------
//  <copyright file="ItemsToTouch.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Database.Impl.DTC
{
    public class ItemsToTouch
    {
        public HashSet<string> Documents = new HashSet<string>();
        public HashSet<string> DocumentTombstones = new HashSet<string>();
    }
}