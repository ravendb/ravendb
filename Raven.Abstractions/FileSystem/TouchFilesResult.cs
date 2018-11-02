// -----------------------------------------------------------------------
//  <copyright file="TouchFilesResult.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;

namespace Raven.Abstractions.FileSystem
{
    public class TouchFilesResult
    {
        public long NumberOfProcessedFiles { get; set; }
        public Etag LastProcessedFileEtag { get; set; }
        public long NumberOfFilteredFiles { get; set; }
        public Etag LastEtagAfterTouch { get; set; }
    }
}