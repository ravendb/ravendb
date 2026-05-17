using System;
using System.Diagnostics;
using System.Net.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.ETL;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class DeleteNamedTaskErrorsCommand : RavenCommand
{
    private readonly StringValues _names;
    private readonly TaskCategory _taskCategory;

    public DeleteNamedTaskErrorsCommand(StringValues names, TaskCategory taskCategory, string nodeTag)
    {
        AssertAllNamesShareSamePrefix(names);

        _names = names;
        _taskCategory = taskCategory;
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/tasks/errors";
        url = QueryHelpers.AddQueryString(url, "type", _taskCategory.ToString());

        foreach (var name in _names)
            url = QueryHelpers.AddQueryString(url, "name", name);

        return new HttpRequestMessage { Method = HttpMethod.Delete };
    }

    [Conditional("DEBUG")]
    internal static void AssertAllNamesShareSamePrefix(StringValues names)
    {
        if (names.Count <= 1)
            return;

        string expectedPrefix = null;

        foreach (var name in names)
        {
            if (name == null)
                continue;

            var slashIndex = name.IndexOf('/');
            var prefix = slashIndex >= 0 ? name[..slashIndex] : name;

            if (expectedPrefix == null)
            {
                expectedPrefix = prefix;
                continue;
            }

            Debug.Assert(string.Equals(prefix, expectedPrefix, StringComparison.Ordinal),
                $"All task names provided to delete-errors must share the same task name prefix. Expected '{expectedPrefix}' but found '{name}'.");
        }
    }
}
