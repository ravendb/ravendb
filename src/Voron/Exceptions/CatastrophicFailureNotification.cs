using System;
using System.Diagnostics;

namespace Voron.Exceptions
{
    public class CatastrophicFailureNotification
    {
        private readonly Action<Exception> _catastrophicFailure;

        public CatastrophicFailureNotification(Action<Exception> catastrophicFailureHandler)
        {
            Debug.Assert(catastrophicFailureHandler != null);

            _catastrophicFailure = catastrophicFailureHandler;
        }

        public void RaiseNotification(Exception e)
        {
            _catastrophicFailure.Invoke(e);
        }
    }
}