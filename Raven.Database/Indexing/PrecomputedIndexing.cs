// -----------------------------------------------------------------------
//  <copyright file="PrecomputedIndexing.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
    public class PrecomputedIndexingBatch
    {
        public Etag LastIndexed;
        public DateTime LastModified;
        public List<JsonDocument> Documents;
        public Index Index;
    }
}
