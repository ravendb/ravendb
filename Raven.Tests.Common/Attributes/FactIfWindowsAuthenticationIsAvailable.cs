using System.Diagnostics;
using System.Net;

using Raven.Tests.Helpers.Util;

using Xunit;

namespace Raven.Tests.Common.Attributes
{
    public static class FactIfWindowsAuthenticationIsAvailable
    {
        private readonly static SkipException SkipException = new SkipException("Cannot execute this test, because this test rely on actual Windows Account name / password.");

        private static bool triedLoading;

        public static NetworkCredential Admin { get; private set; }

        public static NetworkCredential User { get; private set; }

        public static void LoadCredentials()
        {
            if (Admin != null)
                return;

            if (triedLoading)
                throw SkipException;

            lock (typeof(FactIfWindowsAuthenticationIsAvailable))
            {
                triedLoading = true;
                ActualLoad();
                if (Admin == null)
                    throw SkipException;
            }
        }

        [Conditional("DEBUG")]
        private static void LoadDebugCredentials()
        {
            Admin = new NetworkCredential("local_user_test", "local_user_test", "local_machine_name_test");
            User = new NetworkCredential("local_machine_invalid_user", "local_machine_invalid_user", "local_machine_name_test");

            if (Admin.UserName == "local_user_test")
            {
                Admin = null;
            }
        }

        private static void ActualLoad()
        {
            var credentials = ConfigurationHelper.Credentials;

            NetworkCredential adminCredentials;
            if (credentials.TryGetValue("Admin", out adminCredentials))
                Admin = adminCredentials;

            NetworkCredential userCredentials;
            if (credentials.TryGetValue("User", out userCredentials))
                User = userCredentials;
        }
    }
}