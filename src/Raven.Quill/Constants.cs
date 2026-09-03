namespace Raven.Quill;

/// <summary>
/// Contains constants used throughout the Quill appliance.
/// </summary>
public static class Constants
{
    /// <summary>
    /// Names of the environment variables the appliance reads its configuration from. Nothing else
    /// configures it - there is no settings file and no runtime write path - so this is the whole surface.
    /// Every one is read once, in Program.cs, into <see cref="Hosting.ApplianceOptions"/>.
    /// </summary>
    public sealed class Configuration
    {
        private Configuration()
        {
        }

        /// <summary>
        /// URL of the RavenDB server the appliance talks to. Defaults to http://127.0.0.1:8080.
        /// </summary>
        public const string RavenUrl = "RAVEN_QUILL_RAVEN_URL";

        /// <summary>
        /// Address the appliance's own web host binds to. Defaults to http://127.0.0.1:5000.
        /// </summary>
        public const string WebListenUrl = "RAVEN_QUILL_WEB_LISTEN_URL";

        /// <summary>
        /// Database holding the appliance's own configuration documents.
        /// </summary>
        public const string ConfigDatabase = "RAVEN_QUILL_CONFIG_DB";

        /// <summary>
        /// Directory the RavenDB setup package is unpacked into on activation. Defaults to /setup.
        /// </summary>
        public const string SetupPackagePath = "RAVEN_QUILL_SETUP_PACKAGE_PATH";

        /// <summary>
        /// Name of the s6 service supervising RavenDB. Set, the appliance restarts RavenDB through
        /// <c>s6-svc -r</c> after activation; unset, it leaves the restart to the operator.
        /// </summary>
        public const string RavenDbS6Service = "RAVEN_QUILL_RAVENDB_S6_SERVICE";

        /// <summary>
        /// Port the appliance reaches RavenDB on from inside the container, which is not the public one.
        /// Defaults to 8443.
        /// </summary>
        public const string RavenDbInternalPort = "RAVEN_QUILL_RAVENDB_INTERNAL_PORT";

        /// <summary>
        /// How long an AI assistant call may run, in seconds. Defaults to 300.
        /// </summary>
        public const string AiAssistTimeoutSeconds = "RAVEN_QUILL_AI_ASSIST_TIMEOUT_SECONDS";

        /// <summary>
        /// Base URL of the Telegram Bot API. Must be an absolute http(s) URL when set.
        /// </summary>
        public const string TelegramApiUrl = "RAVEN_QUILL_TELEGRAM_API_URL";

        /// <summary>
        /// Base URL of the Slack Web API. Validated as an absolute http(s) URL.
        /// </summary>
        public const string SlackApiUrl = "RAVEN_QUILL_SLACK_API_URL";

        /// <summary>
        /// Base URL of the Discord API. Validated as an absolute http(s) URL.
        /// </summary>
        public const string DiscordApiUrl = "RAVEN_QUILL_DISCORD_API_URL";

        /// <summary>
        /// License key the appliance activates itself with at startup. Unset, it stays in
        /// NeedsActivation.
        /// </summary>
        public const string LicenseKey = "QUILL_LICENSE_KEY";

        /// <summary>
        /// API key accepted on the appliance's own API, alongside the operator cookie.
        /// </summary>
        public const string ApiKey = "QUILL_API_KEY";

        /// <summary>
        /// How long to wait for RavenDB to start before probing readiness, in seconds. Defaults to 15.
        /// </summary>
        public const string ReadinessInitialDelaySeconds = "RAVEN_QUILL_READINESS_INITIAL_DELAY_SECONDS";

        /// <summary>
        /// How long a single readiness probe may take, in seconds. Defaults to 2.
        /// </summary>
        public const string ReadinessAttemptTimeoutSeconds = "RAVEN_QUILL_READINESS_ATTEMPT_TIMEOUT_SECONDS";

        /// <summary>
        /// How long readiness probing continues before giving up, in seconds. Defaults to 30.
        /// </summary>
        public const string ReadinessOverallTimeoutSeconds = "RAVEN_QUILL_READINESS_OVERALL_TIMEOUT_SECONDS";

        /// <summary>
        /// Path to an NLog configuration file. Set, that file is the only thing that configures logging:
        /// <see cref="LogsPath"/>, <see cref="SecurityAuditLogPath"/> and <see cref="LogsMinLevel"/> are
        /// ignored. Unset, /var/lib/quill/quill.nlog.config is used if it exists.
        /// </summary>
        public const string LogsConfigPath = "RAVEN_QUILL_LOGS_CONFIG_PATH";

        /// <summary>
        /// Directory quill.log is written to; unset, the appliance logs to stdout only. Must be absolute.
        /// <para>
        /// Also read by name from quill.nlog.template.config, where NLog resolves it as
        /// <c>${environment:...}</c> - renaming this means editing that file too.
        /// </para>
        /// </summary>
        public const string LogsPath = "RAVEN_QUILL_LOGS_PATH";

        /// <summary>
        /// Directory quill.audit.log is written to; unset, no audit log is kept. Must be absolute.
        /// <para>
        /// Also read by name from quill.nlog.template.config, as <see cref="LogsPath"/> is.
        /// </para>
        /// </summary>
        public const string SecurityAuditLogPath = "RAVEN_QUILL_SECURITY_AUDITLOG_PATH";

        /// <summary>
        /// Lowest level written, one of the <see cref="Sparrow.Logging.LogLevel"/> names. Defaults to
        /// Info. Off silences the normal log but not the audit log, which carries its own level.
        /// </summary>
        public const string LogsMinLevel = "RAVEN_QUILL_LOGS_MINLEVEL";
    }
}
