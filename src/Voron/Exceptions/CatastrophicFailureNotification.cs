using System;
using System.Diagnostics;

namespace Voron.Exceptions
{
    public class CatastrophicFailureNotification
    {
        private readonly Action<Exception> _catastrophicFailure;
        private bool _raised;

        public CatastrophicFailureNotification(Action<Exception> catastrophicFailureHandler)
        {
            Debug.Assert(catastrophicFailureHandler != null);

            _catastrophicFailure = catastrophicFailureHandler;
        }

        public void RaiseNotificationOnce(Exception e)
        {
            if (_raised)
                return;

            _catastrophicFailure.Invoke(e);

            _raised = true;
        }
    }
}