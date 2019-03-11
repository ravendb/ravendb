using System;
using System.Diagnostics;

namespace Voron.Exceptions
{
    public class CatastrophicFailureNotification
    {
        private readonly Action<Guid, string, Exception, string> _catastrophicFailure;
        private bool _raised;

        public CatastrophicFailureNotification(Action<Guid, string, Exception, string> catastrophicFailureHandler)
        {
            Debug.Assert(catastrophicFailureHandler != null);

            _catastrophicFailure = catastrophicFailureHandler;
        }

        public void RaiseNotificationOnce(Guid environmentId, string environmentPath, Exception e, string stacktrace)
        {
            if (_raised)
                return;

            _catastrophicFailure.Invoke(environmentId, environmentPath, e, stacktrace);

            _raised = true;
        }
    }
}
