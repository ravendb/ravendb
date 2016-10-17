//-----------------------------------------------------------------------
// <copyright file="SuggestionQueryExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Database.Plugins;

namespace Raven.Database.Queries
{
    public static class MoreLikeThisQueryExtensions
    {
        public static MoreLikeThisQueryResult ExecuteMoreLikeThisQuery(this DocumentDatabase self, MoreLikeThisQuery query, TransactionInformation transactionInformation, int pageSize, OrderedPartCollection<AbstractIndexQueryTrigger> databaseIndexQueryTriggers)
        {
            return new MoreLikeThisQueryRunner(self).ExecuteMoreLikeThisQuery(query, transactionInformation, pageSize, databaseIndexQueryTriggers);
        }
    }
}
