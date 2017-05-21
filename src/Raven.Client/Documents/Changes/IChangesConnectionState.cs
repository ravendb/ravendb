using System;

namespace Raven.Client.Documents.Changes
{
    internal interface IChangesConnectionState
    {
        void Inc();

        void Dec();

        void Error(Exception e);
    }
}
