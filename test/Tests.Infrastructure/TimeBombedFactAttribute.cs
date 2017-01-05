using System;
using Xunit;

namespace Tests.Infrastructure
{
    public class TimeBombedFactAttribute : FactAttribute
    {
        private readonly string _skipMessage;
        private readonly DateTime _skipUntil;

        public TimeBombedFactAttribute(int year, int month, int day, string skipMessage)
        {
            if (skipMessage == null)
                throw new ArgumentNullException(nameof(skipMessage));

            _skipMessage = skipMessage;
            _skipUntil = new DateTime(year, month, day);
        }

        public override string Skip
        {
            get
            {
                if (DateTime.Now < _skipUntil)
                    return _skipMessage;

                return null;
            }
        }
    }
}