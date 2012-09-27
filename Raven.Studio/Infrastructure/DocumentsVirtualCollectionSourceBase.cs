using Raven.Studio.Features.Documents;

namespace Raven.Studio.Infrastructure
{
    public abstract class DocumentsVirtualCollectionSourceBase : VirtualCollectionSource<ViewableDocument>
    {
        private bool metadataOnly;

        public bool MetadataOnly
        {
            get { return metadataOnly; }
            set
            {
                var needsWholeDocuments = metadataOnly && !value;

                metadataOnly = value;

                if (needsWholeDocuments)
                    Refresh(RefreshMode.PermitStaleDataWhilstRefreshing);
            }
        }
    }
}