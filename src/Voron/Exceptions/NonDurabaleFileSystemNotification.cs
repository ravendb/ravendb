using System;
using System.Diagnostics;

namespace Voron.Exceptions
{
    public class NonDurabaleFileSystemNotification
    {
        private readonly Action<Exception> _nonDurabaleFileSystemAlertHandler;
        private bool _raised;

        public NonDurabaleFileSystemNotification(Action<Exception> nonDurabaleFileSystemAlertHandler)
        {
            Debug.Assert(nonDurabaleFileSystemAlertHandler != null);

            _nonDurabaleFileSystemAlertHandler = nonDurabaleFileSystemAlertHandler;
        }

        public void RaiseNotificationOnce(Exception e)
        {
            if (_raised)
                return;

            _nonDurabaleFileSystemAlertHandler.Invoke(e);

            _raised = true;
        }
    }
}