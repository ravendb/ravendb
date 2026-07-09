using System.Text.Json;

namespace Raven.AiAppliance.Contracts;
public record QuillUsageResponse(
    List<QuillApplicationUsage> PerApplication,
    List<QuillPeriodUsage> ByPeriod);

public record QuillPeriodUsage(
    DateTime From,
    DateTime To,
    long Usage);

public record QuillApplicationUsage(
    string TopologyId,
    string ApplicationName,
    DateTime From,
    DateTime To,
    long Usage);
