// -----------------------------------------------------------------------
//  <copyright file="FileSystemSmugglerOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Common;

namespace Raven.Abstractions.Database.Smuggler.FileSystem
{
    public class FileSystemSmugglerOptions : CommonSmugglerOptions
    {
        public FileSystemSmugglerOptions()
        {
            StartFilesEtag = Etag.Empty;
            StartFilesDeletionEtag = Etag.Empty;
        }
        
        public Etag StartFilesEtag { get; set; }
        public Etag StartFilesDeletionEtag { get; set; } // TODO arek - verify it it make sense for file systems

        public bool StripReplicationInformation { get; set; }

        public bool ShouldDisableVersioningBundle { get; set; }

    }
}