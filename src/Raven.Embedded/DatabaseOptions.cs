using Raven.Client.ServerWide;

namespace Raven.Embedded
{
    public class DatabaseOptions
    {
        public string DatabaseName { get; set; }

        public bool SkipCreatingDatabase { get; set; }

        public DatabaseRecord DatabaseRecord { get; set; }
    }
}
