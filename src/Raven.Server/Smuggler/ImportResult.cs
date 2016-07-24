using System.Collections.Generic;

namespace Raven.Server.Smuggler
{
    public class ImportResult
    {
        public long DocumentsCount;
        public long VersioningRevisionDocumentsCount;
        public long IndexesCount;
        public long TransformersCount;
        public long IdentitiesCount;

        public readonly List<string> Warnings = new List<string>();
    }
}