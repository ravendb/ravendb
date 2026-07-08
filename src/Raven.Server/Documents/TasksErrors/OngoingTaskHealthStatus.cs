using System;

namespace Raven.Server.Documents.TasksErrors;

public enum OngoingTaskHealthStatus
{
    Healthy,
    Impaired,
    Failed
}

public static class OngoingTaskHealthStatusExtensions
{
    public static OngoingTaskHealthStatus FromErrorRatio(double errorRatio, float failedThreshold, float impairedThreshold)
    {
        return errorRatio switch
        {
            _ when errorRatio > failedThreshold => OngoingTaskHealthStatus.Failed,
            _ when errorRatio > impairedThreshold => OngoingTaskHealthStatus.Impaired,
            _ => OngoingTaskHealthStatus.Healthy
        };
    }

    public static void ValidateThresholds(float failedThreshold, float impairedThreshold, string failedThresholdKey, string impairedThresholdKey)
    {
        if (failedThreshold is < 0f or > 1f)
            throw new InvalidOperationException($"The value of '{failedThresholdKey}' ({failedThreshold}) must be between 0 and 1.");

        if (impairedThreshold is < 0f or > 1f)
            throw new InvalidOperationException($"The value of '{impairedThresholdKey}' ({impairedThreshold}) must be between 0 and 1.");

        if (failedThreshold <= impairedThreshold)
            throw new InvalidOperationException(
                $"The value of '{failedThresholdKey}' ({failedThreshold}) must be greater than the value of '{impairedThresholdKey}' ({impairedThreshold}).");
    }
}
