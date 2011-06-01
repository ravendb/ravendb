//-----------------------------------------------------------------------
// <copyright file="HttpListenerResponseAdapter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace Raven.Http.Abstractions
{
    public class HttpListenerResponseAdapter : IHttpResponse
    {
        private readonly HttpListenerResponse response;
    	private readonly Dictionary<string, string> delayedHeaders = new Dictionary<string, string>();

        public HttpListenerResponseAdapter(HttpListenerResponse response)
        {
            this.response = response;
            OutputStream = response.OutputStream;
        }

        public string RedirectionPrefix
        {
            get;
            set;
        }

    	public void AddHeader(string name, string value)
    	{
			if(name == "ETag")
				delayedHeaders["Expires"] = "Sat, 01 Jan 2000 00:00:00 GMT";
    		response.AddHeader(name, value);
    	}

    	private Stream outputStream;
    	public Stream OutputStream
    	{
    		get
    		{
    			FlushHeaders();
    			return outputStream;
    		}
    		set { outputStream = value; }
    	}

    	private void FlushHeaders()
    	{
    		foreach (var delayedHeader in delayedHeaders)
    		{
    			response.AddHeader(delayedHeader.Key, delayedHeader.Value);
    		}
			delayedHeaders.Clear();
    	}

    	public long ContentLength64
        {
            get { return response.ContentLength64; }
            set { response.ContentLength64 = value; }
        }

        public int StatusCode
        {
            get { return response.StatusCode; }
            set { response.StatusCode = value; }
        }

        public string StatusDescription
        {
            get { return response.StatusDescription; }
            set { response.StatusDescription = value; }
        }

        public string ContentType
        {
            get { return response.ContentType; }
            set { response.ContentType = value; }
        }

        public void Redirect(string url)
        {
            response.Redirect(RedirectionPrefix + url);
        }

        public void Close()
        {
            OutputStream.Dispose();
            response.Close();
        }

    	public void WriteFile(string path)
    	{
    		using(var file = File.OpenRead(path))
    		{
    			file.CopyTo(OutputStream);
    		}
    	}

    	public void SetPublicCachability()
    	{
    		response.Headers["Cache-Control"] = "Public";
    	}
    }
}
