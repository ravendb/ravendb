using System;
using System.IO;
using System.Net;

namespace Raven.Imports.SignalR.Client.Http
{
    public class HttpWebResponseWrapper : IResponse
    {
    	private readonly HttpWebRequest _request;
    	private readonly HttpWebResponse _response;

        public HttpWebResponseWrapper(HttpWebRequest request,HttpWebResponse response)
        {
        	this._request = request;
        	_response = response;
        }

    	public string ReadAsString()
        {
            return _response.ReadAsString();   
        }

        public Stream GetResponseStream()
        {
            return _response.GetResponseStream();
        }

        public void Close()
        {
			if (_request != null)
        		_request.Abort();
            ((IDisposable)_response).Dispose();
        }
    }
}
