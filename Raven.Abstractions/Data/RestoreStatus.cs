using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class RestoreStatus
    {
        public const string RavenRestoreStatusDocumentKey = "Raven/Restore/Status";

		public List<string> Messages { get; set; } 
    }
}