// -----------------------------------------------------------------------
//  <copyright file="BuildVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;

namespace Raven.Server.Web.System
{
    public class CorsPreflightHandler : RequestHandler
    {
        public Task HandlePreflightRequest()
        {
            // SetupCORSHeaders is called in generic handler - no need to call it here 
            
            HttpContext.Response.Headers.Remove(Constants.Headers.ContentType);
            
            return Task.CompletedTask;
        }
    }
}
