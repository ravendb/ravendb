//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class DeleteDatabaseResult
    {
        public BlittableJsonReaderObject Results { get; set; }
    }
}
