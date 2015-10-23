using System.Collections.Generic;

using Raven.Abstractions.Data;

namespace Raven.Abstractions.Database.Smuggler.Other
{
    public class ExportFilesDestinations
    {
        public const string RavenDocumentKey = "Raven/Smuggler/Export/Files/Incremental";

        public Dictionary<string, ExportFilesDestinationKey> Destinations { get; set; }

        public ExportFilesDestinations()
        {
            Destinations = new Dictionary<string, ExportFilesDestinationKey>();
        }
    }

    public class ExportFilesDestinationKey
    {
        public Etag LastEtag { get; set; }

        public Etag LastDeletedEtag { get; set; }
    }
}
