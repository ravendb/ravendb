// -----------------------------------------------------------------------
//  <copyright file="PrecomputedIndexing.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
	public class PrecomputedIndexing
	{
        public class Batch
        {
            public Etag LastIndexed;
            public DateTime LastModified;
            public List<JsonDocument> Documents;
            public Index Index;
        }

	    public Task<Batch> RetrieveDocuments { get; set; }

        public Task IndexDocuments { get; set; }
	}
}