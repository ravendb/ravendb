using System;
using System.Net;

namespace Raven.Imports.SignalR.Client.Infrastructure
{
    internal static class ExceptionHelper
    {
        internal static bool IsRequestAborted(Exception exception)
        {
            var webException = exception.Unwrap() as WebException;
            return (webException != null && webException.Status == WebExceptionStatus.RequestCanceled);
        }
    }
}
