//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.NewClient.Abstractions.Data;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class GetSubscriptionsResult
    {
        public BlittableJsonReaderArray Subscriptions;
    }
}
