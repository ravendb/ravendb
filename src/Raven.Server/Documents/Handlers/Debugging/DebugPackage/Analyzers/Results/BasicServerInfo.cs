using System;
using System.Collections.Generic;
using Raven.Server.Config;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class BasicServerInfo
{
    public string NodeTag { get; set; }
    public string Version { get; set; }
    public string ServerId { get; set; }
    public TimeSpan? UpTime { get; set; }
    public DateTime? StartUpTime { get; set; }
}
