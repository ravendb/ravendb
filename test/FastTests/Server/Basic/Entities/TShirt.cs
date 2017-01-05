// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System.Collections.Generic;

namespace FastTests.Server.Basic.Entities
{
    public class TShirt
    {
        public string Id { get; set; }
        public int ReleaseYear { get; set; }
        public string Manufacturer { get; set; }
        public List<TShirtType> Types { get; set; }
    }
}
