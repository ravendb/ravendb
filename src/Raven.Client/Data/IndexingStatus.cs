// -----------------------------------------------------------------------
//  <copyright file="IndexingStatus.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
    public class IndexingStatus
    {
        public string MappingStatus { get; set; }

        public string ReducingStatus { get; set; }
    }
}