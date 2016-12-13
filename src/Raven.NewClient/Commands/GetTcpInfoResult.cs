//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    /// <summary>
    /// The result of a Get tcp info operation
    /// </summary>
    public class GetTcpInfoResult
    {
        public string Url { get; set; }
    }
}
