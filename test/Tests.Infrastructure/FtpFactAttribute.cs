using System;
using System.Runtime.CompilerServices;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace Tests.Infrastructure
{
    public class FtpFactAttribute : FactAttribute
    {
        private const string FtpCredentialEnvironmentVariable = "FTP_CREDENTIAL";

        private static readonly FtpSettings _ftpSettings;

        public static FtpSettings FtpSettings => new FtpSettings(_ftpSettings);

        private static readonly string ParsingError;

        private static readonly bool EnvVariableMissing;

        static FtpFactAttribute()
        {
            var ftpSettingsString = Environment.GetEnvironmentVariable(FtpCredentialEnvironmentVariable);
            if (ftpSettingsString == null)
            {
                EnvVariableMissing = true;
                return;
            }

            try
            {
                _ftpSettings = JsonConvert.DeserializeObject<FtpSettings>(ftpSettingsString);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public FtpFactAttribute([CallerMemberName] string memberName = "")
        {
            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (EnvVariableMissing)
            {
                Skip = $"Test is missing '{FtpCredentialEnvironmentVariable}' environment variable.";
                return;
            }

            if (string.IsNullOrEmpty(ParsingError) == false)
            {
                Skip = $"Failed to parse the Ftp settings, error: {ParsingError}";
                return;
            }

            if (_ftpSettings == null)
            {
                Skip = $"Ftp {memberName} tests missing {nameof(FtpSettings)}.";
                return;
            }
        }
    }
}
