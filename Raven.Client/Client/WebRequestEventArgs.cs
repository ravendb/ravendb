using System;
using System.Net;

namespace Raven.Client.Client
{
    public class WebRequestEventArgs : EventArgs
    {
        public WebRequest Request{ get; set;}
    }
}