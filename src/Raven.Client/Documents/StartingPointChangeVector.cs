using System;

namespace Raven.Client.Documents
{
    public record StartingPointChangeVector
    {
        public readonly string Value;

        private StartingPointChangeVector(string changeVector)
        {
            Value = changeVector ?? throw new ArgumentNullException(nameof(changeVector));
        }

        public static StartingPointChangeVector From(string changeVector) => new(changeVector);

        public static readonly StartingPointChangeVector DoNotChange = From(nameof(DoNotChange));

        public static readonly StartingPointChangeVector LastDocument = From(nameof(LastDocument));

        public static readonly StartingPointChangeVector BeginningOfTime = From(nameof(BeginningOfTime));
    }
}
