using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.Contracts;

namespace Raven.Quill.AiHelper.Migration;

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
    public MigrationProposal Proposal { get; set; } = new();
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

public sealed class DoneFrame : MigrationFrame
{
    [Required]
    public string ConversationId { get; set; } = string.Empty;

    [Required]
    public string PlanId { get; set; } = string.Empty;

    public double? UsagePercentage { get; set; }
}

public abstract class CollectionErrorFrame : MigrationFrame
{
    [Required]
    public string Collection { get; set; } = string.Empty;
    [Required]
    public ValidationIssue[] Errors { get; set; } = [];
}

public sealed class TransientCollectionErrorFrame : CollectionErrorFrame;

public sealed class RejectedCollectionFrame : CollectionErrorFrame
{
    public CdcSinkTableConfig? Config { get; set; }
}

public sealed class ErrorFrame : MigrationFrame
{
    [Required]
    public string Message { get; set; } = string.Empty;

    public string? PlanId { get; set; }

    public string? Code { get; set; }
}

public sealed class MigrationProposal
{
    [Required]
    public ProposedArea[] Areas { get; set; } = [];
    [Required]
    public DroppedTable[] Dropped { get; set; } = [];
    [Required]
    public string[] Enables { get; set; } = [];
}

public sealed class ValidationIssue
{
    [Required]
    public string Message { get; set; } = string.Empty;

    public string? Path { get; set; }

    public string? Table { get; set; }

    public string? Column { get; set; }

    public string? Property { get; set; }
}

public sealed class OpenQuestion
{
    [Required]
    public string Question { get; set; } = string.Empty;

    [Required]
    public string[] Options { get; set; } = [];

    public int? Recommended { get; set; }
}

public sealed class ProposedArea
{
    public string? Area { get; set; }

    [Required]
    public ProposedCollection[] Collections { get; set; } = [];

    public string? Why { get; set; }
}

public sealed class ProposedCollection
{
    public string? Collection { get; set; }

    public string? RootTable { get; set; }

    [Required]
    public AbsorbedTable[] Absorbs { get; set; } = [];

    public string? Why { get; set; }

    public ProposedField[]? Fields { get; set; }
}

public sealed class AbsorbedTable
{
    public string? Table { get; set; }

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
