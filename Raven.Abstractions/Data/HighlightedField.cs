using System.Globalization;
using System.Text.RegularExpressions;

namespace Raven.Abstractions.Data
{
    /// <summary>
    ///     Represent a field highlight options
    /// </summary>
    public sealed class HighlightedField
    {
        private static readonly Regex FieldOptionMatch =
            new Regex(@"^(?<Field>\w+):(?<FragmentLength>\d+),(?<FragmentCount>\d+)$",
#if !SILVERLIGHT
                RegexOptions.Compiled
#else
                RegexOptions.None
#endif
                );

        public HighlightedField(string field, int fragmentLength, int fragmentCount)
        {
            Field = field;
            FragmentLength = fragmentLength;
            FragmentCount = fragmentCount;
        }

        /// <summary>
        ///     Gets or sets the field.
        /// </summary>
        public string Field { get; private set; }

        /// <summary>
        ///     Gets or sets the fragment length.
        /// </summary>
        public int FragmentLength { get; private set; }

        /// <summary>
        ///     Gets or sets a value indicating how many highlight fragments should be created for the field
        /// </summary>
        public int FragmentCount { get; private set; }

        /// <summary>
        ///     Converts the string representation of a field highlighting options to the <see cref="HighlightedField" /> class.
        /// </summary>
        /// <param name="value">
        ///     Field highlighting options
        ///     <example>Text:250,3,&lt;b&gt;,&lt;/b&gt;</example>
        /// </param>
        /// <param name="result"></param>
        /// <returns></returns>
        public static bool TryParse(string value, out HighlightedField result)
        {
            result = null;

            var match = FieldOptionMatch.Match(value);

            if (!match.Success)
                return false;

            var field = match.Groups["Field"].Value;

            if (string.IsNullOrWhiteSpace(field))
                return false;

            int fragmentLength;

            if (!int.TryParse(
                match.Groups["FragmentLength"].Value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out fragmentLength))
                return false;

            int fragmentCount;

            if (!int.TryParse(
                match.Groups["FragmentCount"].Value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out fragmentCount))
                return false;

            result = new HighlightedField(field, fragmentLength, fragmentCount);

            return true;
        }

        public override string ToString()
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                "{0}:{1},{2}",
                this.Field,
                this.FragmentLength,
                this.FragmentCount);
        }

        public HighlightedField Clone()
        {
            return new HighlightedField(Field, FragmentLength, FragmentCount);
        }
    }
}