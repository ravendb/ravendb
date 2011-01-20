namespace Raven.Client.Silverlight.Common
{
    using System;
    using System.Net;

    public class Response<T>
    {
        public HttpStatusCode StatusCode { get; set; }

        public T Data { get; set; }

        public Exception Exception { get; set; }

        public bool IsSuccess
        {
            get { return this.Exception == null; }
        }
    }
}
