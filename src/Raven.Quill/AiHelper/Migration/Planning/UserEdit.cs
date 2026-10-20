namespace Raven.Quill.AiHelper.Migration.Planning;

public enum UserEditKind
{
    Removed,
    Edited,
    Added,
    Renamed
}

/// <summary>
/// A change the operator made to the plan by hand since the planner last finished a turn, so the next turn
/// can tell the model. <see cref="PreviousName"/> is set for a rename only.
/// </summary>
public sealed record UserEdit(string Collection, UserEditKind Kind, string? PreviousName = null);
