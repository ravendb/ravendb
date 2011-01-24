namespace Raven.Management.Client.Silverlight.Common
{
    using System;
    using System.Net;

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class Response<T>
    {
        /// <summary>
        /// 
        /// </summary>
        public HttpStatusCode StatusCode { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public T Data { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Exception Exception { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsSuccess
        {
            get { return Exception == null && Data != null; }
        }

        /// <summary>
        /// 
        /// </summary>
        public abstract AsyncAction Action { get; }
    }
}