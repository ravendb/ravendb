using System;
using System.Diagnostics;

namespace Voron.Exceptions
{
    public class CatastrophicFailureNotification
    {
        private readonly Action<Guid, Exception> _catastrophicFailure;
        private bool _raised;

        public CatastrophicFailureNotification(Action<Guid, Exception> catastrophicFailureHandler)
        {
            Debug.Assert(catastrophicFailureHandler != null);

            _catastrophicFailure = catastrophicFailureHandler;
        }

        public void RaiseNotificationOnce(Guid environmentId, Exception e)
        {
            if (_raised)
                return;

            _catastrophicFailure.Invoke(environmentId, e);

            _raised = true;
        }
    }
}
