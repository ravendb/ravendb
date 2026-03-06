using System;
using System.Text.RegularExpressions;
using Xunit;

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

        private readonly TestResultState _testState;

        public TestOutcomeAnalyzer(TestResultState testState)
        {
            _testState = testState;
        }

        public Exception Exception => _testState.GetException();

        public bool Failed => _testState?.ExceptionMessages?.Length > 0;

        public bool FailedOnAssertion =>
            _testState?.ExceptionTypes?.Length > 0 &&
            _testState.ExceptionTypes[0] != null &&
            _testState.ExceptionTypes[0].StartsWith("Xunit");

        public bool ShouldSaveDebugPackage()
        {
            return false;
        }
    }
}
