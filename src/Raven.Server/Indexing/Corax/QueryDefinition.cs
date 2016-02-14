// -----------------------------------------------------------------------
//  <copyright file="QueryDefinition.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Indexing;
using Raven.Server.Indexing.Corax.Queries;

namespace Raven.Server.Indexing.Corax
{
    public class QueryDefinition
    {
        public class OrderBy
        {
            public string Name;
            public bool Descending;
        }

        public int Take;
        public Query Query;
        public OrderBy[] Sort;
    }   
}