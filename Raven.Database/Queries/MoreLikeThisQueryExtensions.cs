//-----------------------------------------------------------------------
// <copyright file="SuggestionQueryExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;

namespace Raven.Database.Queries
{
    public static class MoreLikeThisQueryExtensions
    {
        public static MoreLikeThisQueryResult ExecuteMoreLikeThisQuery(this DocumentDatabase self, MoreLikeThisQuery query, int pageSize = 25)
        {
            return new MoreLikeThisQueryRunner(self).ExecuteMoreLikeThisQuery(query, pageSize);
        }
    }
}
