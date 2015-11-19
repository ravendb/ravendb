using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
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

            lock (typeof (FactIfWindowsAuthenticationIsAvailable))
            {
                triedLoading = true;
                ActualLoad();
                if (Admin == null || string.IsNullOrEmpty(Admin.UserName))
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
            var fileName = "WindowsAuthenticationCredentials.txt";
            var path = Path.Combine(@"C:\Builds", fileName);
            path = Path.GetFullPath(path);

            if (File.Exists(path) == false)
            {
                LoadDebugCredentials();
                return;
            }

            Admin = new NetworkCredential();
            var lines = File.ReadAllLines(path, Encoding.UTF8);
            var username = lines[0].Split('\\');
            if (username.Length > 1)
            {
                Admin.Domain = username[0];
                Admin.UserName = username[1];
            }
            else
            {
                Admin.UserName = username[0];
            }
            Admin.Password = lines[1];

            User = new NetworkCredential();
            username = lines[2].Split('\\');
            if (username.Length > 1)
            {
                User.Domain = username[0];
                User.UserName = username[1];
            }
            else
            {
                User.UserName = username[0];
            }
            User.Password = lines[3];
        }
    }
}