using System;
using System.Threading.Tasks;

namespace Raven.Client.Changes
{
    internal interface IChangesConnectionState
    {
        Task Task { get; }

        void Inc();

        void Dec();

        void Error(Exception e);
    }
}
