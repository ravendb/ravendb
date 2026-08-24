namespace Raven.Quill.Contracts;

public record QuillUsageResponse(
    List<QuillApplicationUsage> PerApplication,
    List<QuillPeriodUsage> ByPeriod);

public record QuillPeriodUsage(
    DateTime From,
    DateTime To,
    long Usage);

/// <param name="IsSystem">True for the appliance's own configuration database rather than a user-created
/// app. The license server reports every database in the cluster, so this row is charged like any other and
/// is kept in the list — the UI labels it instead of dropping it, so the per-app rows still sum to the total.
/// Defaulted so a license-server payload that predates the flag still deserializes.</param>
public record QuillApplicationUsage(
    string TopologyId,
    string ApplicationName,
    DateTime From,
    DateTime To,
    long Usage,
    bool IsSystem = false);
