using System;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Client.Exceptions;
using Xunit.Sdk;
using XunitLogger;

namespace Tests.Infrastructure.Utils
{
    public class TestOutcomeAnalyzer
    {
        private static readonly Regex[] TimeoutExceptionMessageRegexes =
        {
            new Regex(@"Could not send command \S* from \S* to leader because there is no leader, and we timed out waiting for one after ", RegexOptions.Compiled),
            new Regex(@"Waited too long for the raft command", RegexOptions.Compiled),
            new Regex(@"Waited for \S* for task with index \S* to complete. ", RegexOptions.Compiled),
            new Regex(@"Waited for \S* but didn't get an index notification for \S*.", RegexOptions.Compiled),
            new Regex(@"Something is wrong, throwing to avoid hanging", RegexOptions.Compiled),
            new Regex(@"Waited for \S* but the command was not applied in this time.", RegexOptions.Compiled)
        };
        
        private readonly Context _context;

        public TestOutcomeAnalyzer(Context context)
        {
            _context = context;
        }

        public Exception Exception => _context.TestException;

        public bool Failed => Exception != null;

        public bool FailedOnAssertion => Exception is XunitException;

        public bool ThrewRavenTimeoutException()
        {
            if (Exception is RavenException == false)
                return false;

            var innerException = Exception.InnerException;

            if (innerException == null)
                return false;

            return TimeoutExceptionMessageRegexes.Any(r => r.IsMatch(innerException.Message));
        }
    }
}
