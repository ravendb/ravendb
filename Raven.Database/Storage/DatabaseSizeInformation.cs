// -----------------------------------------------------------------------
//  <copyright file="DatabaseSizeInformation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage
{
    public class DatabaseSizeInformation
    {
        public static DatabaseSizeInformation Empty = new DatabaseSizeInformation();

        public long UsedSizeInBytes { get; set; }

        public long AllocatedSizeInBytes { get; set; }
    }
}