using System.Text.Json.Serialization;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// One line of the NDJSON stream. Every frame corresponds to something that already happened - a
/// tool call that succeeded or failed - so the browser and the model are told the same story.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(ProposalFrame), "proposal")]
[JsonDerivedType(typeof(CollectionFrame), "collection")]
[JsonDerivedType(typeof(RejectedFrame), "rejected")]
[JsonDerivedType(typeof(RemovedFrame), "removed")]
[JsonDerivedType(typeof(ConventionsFrame), "conventions")]
[JsonDerivedType(typeof(NoteFrame), "note")]
[JsonDerivedType(typeof(ReplyFrame), "reply")]
[JsonDerivedType(typeof(DoneFrame), "done")]
[JsonDerivedType(typeof(ErrorFrame), "error")]
public abstract class MigrationFrame;

public sealed class ProposalFrame : MigrationFrame
{
    public ProposedArea[] Areas { get; set; } = [];
    public ProposedCollection[] Collections { get; set; } = [];
    public DroppedTable[] Dropped { get; set; } = [];
    public string[] Enables { get; set; } = [];
}

public sealed class CollectionFrame : MigrationFrame
{
    public string Status { get; set; } = string.Empty;
    public string Collection { get; set; } = string.Empty;
    public int Version { get; set; }
    public string? Rationale { get; set; }
    public CdcSinkTableConfig? Config { get; set; }
    public string[] Warnings { get; set; } = [];
}

public sealed class RejectedFrame : MigrationFrame
{
    public string Collection { get; set; } = string.Empty;
    public string[] Errors { get; set; } = [];
}

public sealed class RemovedFrame : MigrationFrame
{
    public string? Collection { get; set; }
    public string? Reason { get; set; }
}

public sealed class ConventionsFrame : MigrationFrame
{
    public PropertyCase PropertyCase { get; set; }
    public string? PropertyLanguage { get; set; }
    public string? Notes { get; set; }
    public string[] MustReEmit { get; set; } = [];
}

public sealed class NoteFrame : MigrationFrame
{
    public string Text { get; set; } = string.Empty;
}

public sealed class ReplyFrame : MigrationFrame
{
    public string? Reply { get; set; }
    public string[] Gaps { get; set; } = [];
    public OpenQuestion[] OpenQuestions { get; set; } = [];
}

/// <summary>
/// A question as the browser renders it. <see cref="Recommended"/> indexes into
/// <see cref="Options"/>, and is null only when the agent offered no answers at all.
/// </summary>
public sealed record OpenQuestion(string Question, string[] Options, int? Recommended);

public sealed class DoneFrame : MigrationFrame
{
    public string ConversationId { get; set; } = string.Empty;
}

public sealed class ErrorFrame : MigrationFrame
{
    public string Message { get; set; } = string.Empty;
}

public sealed class ProposedArea
{
    public string? Area { get; set; }

    public string[] Collections { get; set; } = [];

    public string? Why { get; set; }
}

public sealed class ProposedCollection
{
    public string? Collection { get; set; }

    public string? RootTable { get; set; }

    public AbsorbedTable[] Absorbs { get; set; } = [];

    public string? Why { get; set; }
}

public sealed class AbsorbedTable
{
    public string? Table { get; set; }

    /// <summary>"Embed" or "Link".</summary>
    public string? How { get; set; }

    public string? Why { get; set; }
}

public sealed class DroppedTable
{
    public string? Table { get; set; }

    public string? Why { get; set; }
}
