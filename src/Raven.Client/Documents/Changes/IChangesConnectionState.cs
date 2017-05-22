using System;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Changes
{
    internal interface IChangesConnectionState : IDisposable
    {
        void Inc();

        void Dec();

        void Error(Exception e);

        Task EnsureSubscribedNow();
    }
}
