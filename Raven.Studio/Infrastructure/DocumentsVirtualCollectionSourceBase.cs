using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
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
                {
                    Refresh(RefreshMode.PermitStaleDataWhilstRefreshing);
                }
            }
        }
    }
}
