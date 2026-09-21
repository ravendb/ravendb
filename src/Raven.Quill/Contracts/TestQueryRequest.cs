using System.Collections.Generic;
using System.Text.Json;

namespace Raven.Quill.Contracts;

public sealed record TestQueryRequest(string Query, Dictionary<string, JsonElement>? Parameters);

public sealed record TestQueryResponse(JsonElement[] Results, long TotalResults, bool IsTruncated, long DurationMs);
