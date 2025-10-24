namespace Raven.Server.Documents.Patch;

/// <summary>
/// Represents a <see cref="PatchRequest"/> that resolves a conflict between documents.
/// </summary>
public sealed class ConflictPatchRequest : ScriptRunnerCache.Key
{
    private readonly string _script;

    public ConflictPatchRequest(string script)
    {
        _script = script;
    }

    public override void GenerateScript(ScriptRunner runner)
    {
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
