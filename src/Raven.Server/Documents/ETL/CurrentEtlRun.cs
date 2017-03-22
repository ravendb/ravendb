using System.Diagnostics;

namespace Raven.Server.Documents.ETL
{
    public class CurrentEtlRun
    {
        public int NumberOfExtractedItems;

        public long LastTransformedEtag;

        public long LastLoadedEtag;

        public Stopwatch Duration = new Stopwatch();

        public void Reset()
        {
            Duration.Restart();

            NumberOfExtractedItems = 0;
            LastTransformedEtag = 0;
            LastLoadedEtag = 0;
        }

        public void Stop()
        {
            Duration.Stop();
        }
    }
}