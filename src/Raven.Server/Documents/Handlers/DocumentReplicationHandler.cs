using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Server.Documents.Replication;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentReplicationRequestHandler : DatabaseRequestHandler
    {
		[RavenAction("/replication/topology", "GET")]
		[RavenAction("/databases/*/replication/topology", "GET")]
		public Task GetReplicationTopology()
		{
			HttpContext.Response.StatusCode = 404;
			return Task.CompletedTask;
		}
    }
}
