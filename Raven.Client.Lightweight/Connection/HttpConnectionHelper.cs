// -----------------------------------------------------------------------
//  <copyright file="HttpConnectionHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Connection
{
    public static class HttpConnectionHelper
    {
        public static bool IsHttpStatus(Exception e,out HttpStatusCode code,  params HttpStatusCode[] httpStatusCode)
        {
            code = HttpStatusCode.InternalServerError;
            var aggregateException = e as AggregateException;
            if (aggregateException != null)
            {
                e = aggregateException.ExtractSingleInnerException();
            }

            var ere = e as ErrorResponseException ?? e.InnerException as ErrorResponseException;
            if (ere != null)
            {
                code = ere.StatusCode;
                return httpStatusCode.Contains(ere.StatusCode);
            }

#if !DNXCORE50
            var webException = (e as WebException) ?? (e.InnerException as WebException);
            if (webException != null)
            {
                var httpWebResponse = webException.Response as HttpWebResponse;
                if (httpWebResponse != null && httpStatusCode.Contains(httpWebResponse.StatusCode))
                {
                    code = ere.StatusCode;
                    return true;
                }
            }
#endif

            return false;
        }

        public static bool IsServerDown(Exception e, out bool timeout)
        {
            timeout = false;

            var aggregateException = e as AggregateException;
            if (aggregateException != null)
            {
                e = aggregateException.ExtractSingleInnerException();
            }

            var ere = e as ErrorResponseException ?? e.InnerException as ErrorResponseException;
            if (ere != null)
            {
                if (IsServerDown(ere.StatusCode, out timeout))
                    return true;
            }

#if !DNXCORE50
            var webException = (e as WebException) ?? (e.InnerException as WebException);
            if (webException != null)
            {
                switch (webException.Status)
                {
                    case WebExceptionStatus.Timeout:
                        timeout = true;
                        return true;
                    case WebExceptionStatus.NameResolutionFailure:
                    case WebExceptionStatus.ReceiveFailure:
                    case WebExceptionStatus.PipelineFailure:
                    case WebExceptionStatus.ConnectionClosed:
                    case WebExceptionStatus.ConnectFailure:
                    case WebExceptionStatus.SendFailure:
                        return true;
                }

                var httpWebResponse = webException.Response as HttpWebResponse;
                if (httpWebResponse != null)
                {
                    if (IsServerDown(httpWebResponse.StatusCode, out timeout))
                        return true;
                }
            }
#endif

            return e.InnerException is SocketException || e.InnerException is IOException;
        }

        private static bool IsServerDown(HttpStatusCode httpStatusCode, out bool timeout)
        {
            timeout = false;
            switch (httpStatusCode)
            {
                case HttpStatusCode.RequestTimeout:
                case HttpStatusCode.GatewayTimeout:
                    timeout = true;
                    return true;
                case HttpStatusCode.BadGateway:
                case HttpStatusCode.ServiceUnavailable:
                    return true;
            }
            return false;
        }
    }
}
