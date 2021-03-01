using Raven.Client.Documents.Operations.ETL.OLAP;
using Sparrow.Logging;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public abstract class OlapTransformedItems
    {
        protected OlapTransformedItems(OlapEtlFileFormat format)
        {
            Format = format;
        }

        public OlapEtlFileFormat Format { get; }

        public abstract void AddItem(ToOlapItem item);

        public abstract int GenerateFileFromItems(string path, Logger logger = null);

        public abstract string Prefix { get; }

    }
}
