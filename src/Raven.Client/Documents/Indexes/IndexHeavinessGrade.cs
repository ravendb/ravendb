using System.Collections.Generic;

namespace Raven.Client.Documents.Indexes
{
    public enum IndexStaticGrade
    {
        Simple,
        Moderate,
        Complex,
        VeryComplex
    }

    public enum IndexFullGrade
    {
        Lightweight,
        Moderate,
        Heavy,
        VeryHeavy,
        Extreme
    }

    /// <summary>
    /// Represents the computed heaviness grade for an index, quantifying its expected resource impact.
    /// </summary>
    public sealed class IndexHeavinessGrade
    {
        /// <summary>
        /// Static score based solely on the index definition. Server-independent and additive.
        /// Can be compared across different servers and environments.
        /// </summary>
        public int StaticScore { get; set; }

        /// <summary>
        /// Full score that takes into account data scale (collection size, average document size)
        /// and runtime observations. Reflects actual resource impact on this specific server.
        /// Formula: StaticScore × DataScaleMultiplier + RuntimePenalties
        /// </summary>
        public double FullScore { get; set; }

        /// <summary>
        /// Grade label for the static score.
        /// </summary>
        public IndexStaticGrade StaticGradeLabel { get; set; }

        /// <summary>
        /// Grade label for the full score.
        /// </summary>
        public IndexFullGrade FullGradeLabel { get; set; }

        /// <summary>
        /// Individual penalty contributions that made up the static score.
        /// </summary>
        public List<IndexHeavinessPenalty> StaticPenalties { get; set; }

        /// <summary>
        /// Individual penalty contributions from runtime observations.
        /// </summary>
        public List<IndexHeavinessPenalty> RuntimePenalties { get; set; }

        /// <summary>
        /// Data scale multiplier applied to the static score (CollectionSizeFactor × DocumentSizeFactor).
        /// A value of 1.0 is the baseline (1K–10K documents, 1–10KB average size).
        /// </summary>
        public double DataScaleMultiplier { get; set; }
    }

    /// <summary>
    /// Describes a single penalty contribution to an index heaviness score.
    /// </summary>
    public sealed class IndexHeavinessPenalty
    {
        /// <summary>
        /// Short description of the penalty reason.
        /// </summary>
        public string Reason { get; set; }

        /// <summary>
        /// The numeric value of this penalty.
        /// </summary>
        public double Score { get; set; }
    }
}
