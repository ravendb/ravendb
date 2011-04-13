using System;
using System.Windows;

namespace Raven.Studio.Framework
{
    public interface IBindablePagedQuery
    {
        Size? ItemElementSize { get; set; }
        Size PageElementSize { get; set; }
        void AdjustResultsForPageSize();
        void ClearResults();
        event EventHandler<EventArgs<bool>> IsLoadingChanged;
    }
}