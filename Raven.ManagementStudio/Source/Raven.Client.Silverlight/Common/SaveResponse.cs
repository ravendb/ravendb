namespace Raven.Client.Silverlight.Common
{
    using System;
    using System.Net;
    using Raven.Client.Silverlight.Data;

    public class SaveResponse
    {
        public HttpStatusCode StatusCode { get; set; }

        public Entity Entity { get; set; }

        public Exception Exception { get; set; }
    }
}
