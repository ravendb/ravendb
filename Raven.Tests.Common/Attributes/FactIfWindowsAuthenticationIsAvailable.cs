using System.Diagnostics;
using System.IO;
using System.Text;
using Xunit;

namespace Raven.Tests.Common.Attributes
{
    public static class FactIfWindowsAuthenticationIsAvailable
    {
        private static bool triedLoading = false;

        public static string Username { get; private set; }
        public static string Domain { get; private set; }
        public static string Password { get; private set; }

        public static string InvalidUsername { get; private set; }
        public static string InvalidPassword { get; private set; }
        public static string InvalidDomain { get; private set; }

        public static void LoadCredentials()
        {
            if (Username != null)
                return;

            var skipException = new SkipException("Cannot execute this test, because this test rely on actual Windows Account name / password.");
            if (triedLoading)
                throw skipException;

            lock (typeof (FactIfWindowsAuthenticationIsAvailable))
            {
                triedLoading = true;
                ActualLoad();
                if (Username == null)
                    throw skipException;
            }
        }

        [Conditional("DEBUG")]
        private static void LoadDebugCredentials()
        {
            Username = "local_user_test";
            Password = "local_user_test";
            Domain = "local_machine_name_test";
            InvalidUsername = "local_machine_invalid_user";
            InvalidPassword = "local_machine_invalid_user";
            InvalidDomain = "local_machine_name_test";

            if (Username == "local_user_test")
            {
                Username = null;
            }
        }

        private static void ActualLoad()
        {
            var fileName = "WindowsAuthenticationCredentials.txt";
            var path = Path.Combine(@"C:\Builds", fileName);
            path = Path.GetFullPath(path);

            if (File.Exists(path) == false)
            {
                LoadDebugCredentials();
                return;
            }

            var lines = File.ReadAllLines(path, Encoding.UTF8);
            var username = lines[0].Split('\\');
            if (username.Length > 1)
            {
                Domain = username[0];
                Username = username[1];
            }
            else
            {
                Username = username[0];
            }
            Password = lines[1];

            username = lines[2].Split('\\');
            if (username.Length > 1)
            {
                InvalidDomain = username[0];
                InvalidUsername = username[1];
            }
            else
            {
                InvalidUsername = username[0];
            }
            InvalidPassword = lines[3];
        }
    }
}