using System;

namespace Raven.Client.Documents.Queries
{
    public enum ProjectionBehavior
    {
        /// <summary>
        /// Try to extract value from index field (if field value is stored in index) on a failure (or when field value is not stored in index) extract value from a document
        /// </summary>
        Default,

        /// <summary>
        /// Try to extract value from index field (if field value is stored in index)
        /// </summary>
        FromIndex,

        /// <summary>
        /// Extract value from index field or throw
        /// </summary>
        FromIndexOrThrow,

        /// <summary>
        /// Try to extract value from document field
        /// </summary>
        FromDocument,

        /// <summary>
        /// Extract value from document field or throw
        /// </summary>
        FromDocumentOrThrow
    }

    internal static class ProjectionBehaviorExtensions
    {
        public static bool FromIndexOrDefault(this ProjectionBehavior? projectionBehavior)
        {
            if (projectionBehavior == null)
                return true;

            switch (projectionBehavior.Value)
            {
                case ProjectionBehavior.FromDocument:
                case ProjectionBehavior.FromDocumentOrThrow:
                    return false;
                case ProjectionBehavior.Default:
                case ProjectionBehavior.FromIndex:
                case ProjectionBehavior.FromIndexOrThrow:
                    return true;
                default:
                    throw new NotSupportedException($"Not supported projection behavior '{projectionBehavior.Value}'.");
            }
        }

        public static bool FromIndexOnly(this ProjectionBehavior? projectionBehavior)
        {
            if (projectionBehavior == null)
                return false;

            switch (projectionBehavior.Value)
            {
                case ProjectionBehavior.Default:
                case ProjectionBehavior.FromDocument:
                case ProjectionBehavior.FromDocumentOrThrow:
                    return false;
                case ProjectionBehavior.FromIndex:
                case ProjectionBehavior.FromIndexOrThrow:
                    return true;
                default:
                    throw new NotSupportedException($"Not supported projection behavior '{projectionBehavior.Value}'.");
            }
        }

        public static bool FromDocumentOrDefault(this ProjectionBehavior? projectionBehavior)
        {
            if (projectionBehavior == null)
                return true;

            switch (projectionBehavior.Value)
            {
                case ProjectionBehavior.Default:
                case ProjectionBehavior.FromDocument:
                case ProjectionBehavior.FromDocumentOrThrow:
                    return true;
                case ProjectionBehavior.FromIndex:
                case ProjectionBehavior.FromIndexOrThrow:
                    return false;
                default:
                    throw new NotSupportedException($"Not supported projection behavior '{projectionBehavior.Value}'.");
            }
        }

        public static bool FromDocumentOnly(this ProjectionBehavior? projectionBehavior)
        {
            if (projectionBehavior == null)
                return false;

            switch (projectionBehavior.Value)
            {
                case ProjectionBehavior.FromDocument:
                case ProjectionBehavior.FromDocumentOrThrow:
                    return true;
                case ProjectionBehavior.Default:
                case ProjectionBehavior.FromIndex:
                case ProjectionBehavior.FromIndexOrThrow:
                    return false;
                default:
                    throw new NotSupportedException($"Not supported projection behavior '{projectionBehavior.Value}'.");
            }
        }

        public static bool MustThrow(this ProjectionBehavior? projectionBehavior)
        {
            if (projectionBehavior == null)
                return false;

            switch (projectionBehavior.Value)
            {
                case ProjectionBehavior.FromDocumentOrThrow:
                case ProjectionBehavior.FromIndexOrThrow:
                    return true;
                case ProjectionBehavior.FromDocument:
                case ProjectionBehavior.Default:
                case ProjectionBehavior.FromIndex:
                    return false;
                default:
                    throw new NotSupportedException($"Not supported projection behavior '{projectionBehavior.Value}'.");
            }
        }
    }
}
