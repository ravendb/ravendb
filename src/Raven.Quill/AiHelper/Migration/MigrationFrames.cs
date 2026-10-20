using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// One line of the NDJSON stream. Every frame corresponds to something that already happened - a
/// tool call that succeeded or failed - so the browser and the model are told the same story.
/// <see cref="RequiredAttribute"/> marks what every written frame carries, so the browser's generated
/// types can rely on it. Nothing validates it: the planner leaves nulls out of what it writes, and a
/// field it leaves out keeps its default.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(ProposalFrame), "proposal")]
[JsonDerivedType(typeof(CollectionFrame), "collection")]
[JsonDerivedType(typeof(TransientCollectionErrorFrame), "transientCollectionError")]
[JsonDerivedType(typeof(RejectedCollectionFrame), "rejectedCollection")]
[JsonDerivedType(typeof(RemovedFrame), "removed")]
[JsonDerivedType(typeof(ConventionsFrame), "conventions")]
[JsonDerivedType(typeof(NoteFrame), "note")]
[JsonDerivedType(typeof(ReplyFrame), "reply")]
[JsonDerivedType(typeof(DoneFrame), "done")]
[JsonDerivedType(typeof(ErrorFrame), "error")]
public abstract class MigrationFrame;

public sealed class ProposalFrame : MigrationFrame
{
    [Required]
    public ProposedArea[] Areas { get; set; } = [];
    [Required]
    public ProposedCollection[] Collections { get; set; } = [];
    [Required]
    public DroppedTable[] Dropped { get; set; } = [];
    [Required]
    public string[] Enables { get; set; } = [];
}

public sealed class CollectionFrame : MigrationFrame
{
    [Required]
    public string Status { get; set; } = string.Empty;
    [Required]
    public string Collection { get; set; } = string.Empty;
    [Required]
    public int Version { get; set; }
    public string? Rationale { get; set; }
    public CdcSinkTableConfig? Config { get; set; }
    [Required]
    public string[] Warnings { get; set; } = [];
}

/// <summary>A collection that failed validation, with the errors located in its mapping.</summary>
public abstract class CollectionErrorFrame : MigrationFrame
{
    [Required]
    public string Collection { get; set; } = string.Empty;
    [Required]
    public ValidationIssue[] Errors { get; set; } = [];
}

/// <summary>
/// An attempt at a collection failed and the planner is still working on it. <see cref="Rejections"/> counts
/// the attempts that used up one of the <see cref="MaxRejections"/>.
/// </summary>
public sealed class TransientCollectionErrorFrame : CollectionErrorFrame
{
    [Required]
    public int Rejections { get; set; }
    [Required]
    public int MaxRejections { get; set; }
}

/// <summary>
/// The planner gave up on a collection in this turn; <see cref="Config"/> is the mapping it generated last.
/// </summary>
public sealed class RejectedCollectionFrame : CollectionErrorFrame
{
    public CdcSinkTableConfig? Config { get; set; }
}

/// <summary>
/// A validation error located in a collection's configuration. <see cref="Path"/> addresses the offending
/// member in camelCase (e.g. <c>embeddedTables[0].columns[2].column</c>), and is null for the whole configuration.
/// </summary>
public sealed class ValidationIssue
{
    [Required]
    public string Message { get; set; } = string.Empty;

    public string? Path { get; set; }

    public string? Table { get; set; }

    public string? Column { get; set; }

    public string? Property { get; set; }
}

public sealed class RemovedFrame : MigrationFrame
{
    public string? Collection { get; set; }
    public string? Reason { get; set; }
}

public sealed class ConventionsFrame : MigrationFrame
{
    [Required]
    public PropertyCase PropertyCase { get; set; }
    public string? PropertyLanguage { get; set; }
    public string? Notes { get; set; }
    [Required]
    public string[] MustReEmit { get; set; } = [];
}

public sealed class NoteFrame : MigrationFrame
{
    [Required]
    public string Text { get; set; } = string.Empty;
}

public sealed class ReplyFrame : MigrationFrame
{
    public string? Reply { get; set; }
    public string[]? Gaps { get; set; }
    public OpenQuestion[]? OpenQuestions { get; set; }
}

/// <summary>
/// A question as the browser renders it. <see cref="Recommended"/> indexes into
/// <see cref="Options"/>, and is null only when the agent offered no answers at all.
/// </summary>
public sealed class OpenQuestion
{
    [Required]
    public string Question { get; set; } = string.Empty;

    [Required]
    public string[] Options { get; set; } = [];

    public int? Recommended { get; set; }
}

public sealed class DoneFrame : MigrationFrame
{
    [Required]
    public string ConversationId { get; set; } = string.Empty;
}

public sealed class ErrorFrame : MigrationFrame
{
    [Required]
    public string Message { get; set; } = string.Empty;
}

public sealed class ProposedArea
{
    public string? Area { get; set; }

    [Required]
    public string[] Collections { get; set; } = [];

    public string? Why { get; set; }
}

public sealed class ProposedCollection
{
    public string? Collection { get; set; }

    public string? RootTable { get; set; }

    [Required]
    public AbsorbedTable[] Absorbs { get; set; } = [];

    public string? Why { get; set; }

    /// <summary>The RavenDB property names of its root and embedded tables' columns, derived by the planner service.</summary>
    [Required]
    public ProposedField[] Fields { get; set; } = [];
}

public sealed class AbsorbedTable
{
    public string? Table { get; set; }

    /// <summary>"Embed" or "Link".</summary>
    public string? How { get; set; }

    public string? Why { get; set; }
}

public sealed class ProposedField
{
    [Required]
    public string Table { get; set; } = string.Empty;

    [Required]
    public string Column { get; set; } = string.Empty;

    [Required]
    public string Property { get; set; } = string.Empty;
}

public sealed class DroppedTable
{
    public string? Table { get; set; }

    public string? Why { get; set; }
}
