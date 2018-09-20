using System;
using System.Diagnostics;

namespace Voron.Exceptions
{
    public class CatastrophicFailureNotification
    {
        private readonly Action<Guid, string, Exception> _catastrophicFailure;
        private bool _raised;

        public CatastrophicFailureNotification(Action<Guid, string, Exception> catastrophicFailureHandler)
        {
            Debug.Assert(catastrophicFailureHandler != null);

            _catastrophicFailure = catastrophicFailureHandler;
        }

        public void RaiseNotificationOnce(Guid environmentId, string environmentPath, Exception e)
        {
            if (_raised)
                return;

            _catastrophicFailure.Invoke(environmentId, environmentPath, e);

            _raised = true;
        }
    }
}
