using System;
using Voron.Trees;

namespace Voron.Impl.Paging
{
    public interface IFileWriter  :IDisposable
    {
        void Write(Page page);

        void Sync();
        void EnsureContinuous(long pageNumber, int numberOfPagesInLastPage);
    }
}