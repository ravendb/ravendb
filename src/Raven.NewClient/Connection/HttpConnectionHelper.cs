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

namespace Raven.NewClient.Client.Connection
{
    public static class HttpConnectionHelper
    {
        public static bool IsHttpStatus(Exception e, params HttpStatusCode[] httpStatusCode)
        {
            var aggregateException = e as AggregateException;
            if (aggregateException != null)
            {
                e = aggregateException.ExtractSingleInnerException();
            }

            var ere = e as ErrorResponseException ?? e.InnerException as ErrorResponseException;
            if (ere != null)
            {
                return httpStatusCode.Contains(ere.StatusCode);
            }

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
