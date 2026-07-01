namespace Corax.Querying.Planning;

/// <summary> Execution-specific intermediate values used to calculate and calibrate a range clause's estimate. </summary>
public struct RangeEstimateBreakdown
{
    /// <summary>Final calibrated estimate used by cost gates.</summary>
    public long Estimate;

    /// <summary>Pre-calibration estimate (cold-start blend) fed into the EWMA.</summary>
    public long RawEstimate;

    /// <summary>Total term count within the range.</summary>
    public long RangeTerms;

    public long SampledTerms;

    public long SampledPostings;

    /// <summary>RangeTerms - SampledTerms</summary>
    public long MiddleTerms;

    /// <summary>SampledPostings / SampledTerms</summary>
    public double SampledAvg;

    /// <summary>Field-wide density (NumberOfEntries / TotalTerms).</summary>
    public double GlobalAvg;

    /// <summary>EWMA of matched/estimated history; 0 = no history.</summary>
    public double CalibrationFactor;

    /// <summary>Clamped shrinkage strength used in the blend (folded into [0.25, 4.0]).</summary>
    public double Beta;

    /// <summary>Pseudo-observation count (Beta * MiddleTerms).</summary>
    public double K;

    /// <summary>The blended density assigned to the unscanned middle terms.</summary>
    public double MiddleAvg;

    /// <summary>True if the range was small enough to count exactly without extrapolation.</summary>
    public bool IsExact;
}
