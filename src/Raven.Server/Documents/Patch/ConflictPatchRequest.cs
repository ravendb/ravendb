using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Jint.Native;

namespace Raven.Server.Documents.Patch;

/// <summary>
/// Represents a <see cref="PatchRequest"/> that resolves a conflict between documents.
/// </summary>
public sealed class ConflictPatchRequest : ScriptRunnerCache.Key
{
    private readonly string _script;

    /// <summary>
    /// Functions that are forbidden when performing a conflict resolution script.
    /// </summary>
    private static readonly string[] ForbiddenFunctionsNames =
    [
        "put",
        "del",
        "load",
        "loadPath",
    ];

    private static readonly (string name, Func<JsValue, JsValue[], JsValue> func)[] ForbiddenFunctions =
        ForbiddenFunctionsNames
            .Select<string, (string name, Func<JsValue, JsValue[], JsValue> func)>(name => (name, (_, _) => throw new InvalidOperationException($"Calling '{name}' in conflict resolution script is forbidden.")))
            .ToArray();
    
    public ConflictPatchRequest(string script)
    {
        _script = script;
    }

    public override void GenerateScript(ScriptRunner runner)
    {
        // override forbidden
        foreach ((string name, Func<JsValue, JsValue[], JsValue> func) in ForbiddenFunctions)
        {
            runner.SetClrFunction(name, func);
        }
        
        runner.AddScript($@"
function resolve(docs, hasTombstone, resolveToTombstone){{ 

{_script}

}}");
    }

    public override bool Equals(object obj)
    {
        if (ReferenceEquals(null, obj))
            return false;
        if (ReferenceEquals(this, obj))
            return true;
        if (obj.GetType() != this.GetType())
            return false;
        return Equals((ConflictPatchRequest)obj);
    }

    private bool Equals(ConflictPatchRequest other) => string.Equals(_script, other._script);

    public override int GetHashCode() => _script != null ? _script.GetHashCode() : 0;
}
