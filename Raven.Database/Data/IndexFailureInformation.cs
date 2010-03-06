using System;

namespace Raven.Database.Data
{
    public class IndexFailureInformation
    {
        public bool IsInvalidIndex
        {
            get
            {

                if (Errors == 0)
                    return false;
                // we don't have enough attempts to make a useful determination
                if (Attempts < 10)
                    return false;
                return (Attempts/(float) Errors) > 0.15;
            }
        }
        public string Name { get; set; }
        public int Attempts { get; set; }
        public int Errors { get; set; }
        public int Successes { get; set; }

        public string GetErrorMessage()
        {
            if(IsInvalidIndex== false)
                return null;
            return
                string.Format(
                    "Index {0} is invalid, out of {1} indexing attempts, {2} has failed.\r\nError rate of {3:#.##%} exceeds allowed 15% error rate",
                    Name, Attempts, Errors, (Attempts/(float) Errors));
        }
    }
}