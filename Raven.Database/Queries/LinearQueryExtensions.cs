//-----------------------------------------------------------------------
// <copyright file="LinearQueryExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Database.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Queries
{
    public static class LinearQueryExtensions
    {
        public static  QueryResults ExecuteQueryUsingLinearSearch(this DocumentDatabase self,LinearQuery query)
        {
            var linearQueryRunner = (LinearQueryRunner)self.ExtensionsState.GetOrAddAtomically(typeof(LinearQueryExtensions), o => new LinearQueryRunner(self));
            return linearQueryRunner.ExecuteQueryUsingLinearSearch(query);
        }
    }
}