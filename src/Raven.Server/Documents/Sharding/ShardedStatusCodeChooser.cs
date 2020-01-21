using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Raven.Server.Documents.Handlers;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Sharding
{
    public static class ShardedStatusCodeChooser
    {
        public static HttpStatusCode GetStatusCode(IEnumerable<ShardedCommand> commands)
        {
            HttpStatusCode statusCode = HttpStatusCode.Ambiguous;
            bool first = true;
            foreach (var command in commands)
            {
                if (first)
                {
                    statusCode = command.StatusCode;
                    first = false;
                }
                else
                {
                    if (statusCode != command.StatusCode)
                    {
                        //If both statuses are successful we should try to return a successful status code
                        if (statusCode.IsSuccessStatusCode() == false || command.StatusCode.IsSuccessStatusCode() == false)
                        {
                            return HttpStatusCode.Ambiguous;
                        }
                    }
                }
            }
            return statusCode;
        }
    }
}
