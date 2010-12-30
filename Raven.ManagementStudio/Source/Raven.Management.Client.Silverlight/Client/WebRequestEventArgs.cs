namespace Raven.Management.Client.Silverlight.Client
{
    using System;
    using System.Net;

    /// <summary>
    /// Event arguments for the event of creating a <see cref="WebRequest"/>
    /// </summary>
    public class WebRequestEventArgs : EventArgs
    {
        /// <summary>
        /// Gets or sets the web request.
        /// </summary>
        /// <value>The request.</value>
        public WebRequest Request { get; set; }
    }
}