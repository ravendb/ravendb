// -----------------------------------------------------------------------
//  <copyright file="BuildVersion.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.BackgroundTasks;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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
