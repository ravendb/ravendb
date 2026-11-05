using System;
using System.Threading;

namespace Corax.Querying.Planning;

/// <summary>
/// Thread-safe exponentially-weighted moving average of an <em>inflation factor</em>: the ratio of
/// what actually happened to what a heuristic predicted (<c>actual / predicted</c>). It is a smoothed,
/// self-correcting multiplier that lets a cheap a-priori estimate learn from the outcomes it produced,
/// without any per-run state beyond a single double.
///
/// - Streaming-sort scan inflation - the number of scanned index entries vs. the expected number
/// - Range-estimate calibration - documents within a range vs. the estimated value
///
/// The instances is shared among all query executions. Writes are rare (once per observed run);
/// reads are on hot paths and may read stale value. The race between a reader and a writer is benign: on 64-bit
/// the read can't see torn values and on 32-bit a torn read at worst nudges one query's estimate — acceptable for a
/// self-correcting heuristic.
/// </summary>
public sealed class InflationEwma
{
    // how much an update value shifts the state
    private const double Alpha = 0.05;

    // skip updating the metric if we are within 1% of the value, to avoid lock cmpxcng
    private const double ConvergenceTolerance = 0.01;

    // by how much each change move the current value
    private double _factor;

    // 0 is a neutral value, means - don't change your estimate
    public double Factor => _factor;

    /// <summary>
    /// We care here about the difference between the predicted and the actual values.
    /// We'll use this rate to adjust the current metric based on that ratio. 
    /// </summary>
    public void Observe(long actual, long predicted)
    {
        if (predicted <= 0)
            return;

        double sample = (double)actual / predicted;

        while (true)
        {
            double current = _factor;
            double updated = current == 0
                ? sample
                : current + Alpha * (sample - current);

            // Converged: the update would move the factor by less than ConvergenceTolerance, no need to update
            if (current != 0 && Math.Abs(updated - current) <= current * ConvergenceTolerance)
                return;

            // ReSharper disable once CompareOfFloatsByEqualityOperator
            if (Interlocked.CompareExchange(ref _factor, updated, current) == current)
                return;
        }
    }
}
