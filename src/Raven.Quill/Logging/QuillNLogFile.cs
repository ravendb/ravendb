using System.Xml.Linq;
using Sparrow.Global;

namespace Raven.Quill.Logging;

internal static class QuillNLogFile
{
    private const string RuleElement = "logger";
    private const string TargetElement = "target";
    private const string RuleNameAttribute = "ruleName";
    private const string NameAttribute = "name";
    private const string MinLevelAttribute = "minlevel";
    private const string FinalMinLevelAttribute = "finalMinLevel";
    private const string WriteToAttribute = "writeTo";
    private const string FileNameAttribute = "fileName";

    internal static void Persist(QuillLogging logging)
    {
        ArgumentNullException.ThrowIfNull(logging);

        var configPath = logging.ConfigPath
                         ?? throw new InvalidOperationException("no configuration file path is configured.");

        if (File.Exists(configPath) == false)
            Seed(configPath);

        var document = XDocument.Load(configPath, LoadOptions.PreserveWhitespace);
        var ns = document.Root?.Name.Namespace
                 ?? throw new InvalidOperationException($"'{configPath}' is not an NLog configuration.");

        Patch(document, ns, logging);

        Write(configPath, document);
    }

    private static void Seed(string configPath)
    {
        var template = Path.Combine(AppContext.BaseDirectory, QuillLogging.TemplateFileName);

        if (File.Exists(template) == false)
            throw new InvalidOperationException(
                $"'{template}' is missing from the installation, so there is nothing to base " +
                $"'{configPath}' on.");

        var directory = Path.GetDirectoryName(configPath);

        if (string.IsNullOrEmpty(directory) == false)
            Directory.CreateDirectory(directory);

        File.Copy(template, configPath);
    }

    private static void Patch(XDocument document, XNamespace ns, QuillLogging logging)
    {
        var finalMinLevel = logging.MicrosoftFinalMinLevel?.ToString();

        Rule(document, ns, Constants.Logging.Names.MicrosoftRuleName)
            .SetAttributeValue(FinalMinLevelAttribute, finalMinLevel);
        Rule(document, ns, Constants.Logging.Names.SystemRuleName)
            .SetAttributeValue(FinalMinLevelAttribute, finalMinLevel);

        var auditRule = Rule(document, ns, Constants.Logging.Names.DefaultAuditRuleName);
        auditRule.SetAttributeValue(WriteToAttribute, WriteTo(logging.AuditTargetNames));

        var defaultRule = Rule(document, ns, Constants.Logging.Names.DefaultRuleName);
        defaultRule.SetAttributeValue(MinLevelAttribute, logging.CurrentMinLevel.ToString());
        defaultRule.SetAttributeValue(WriteToAttribute, WriteTo(logging.DefaultTargetNames));

        if (logging.CurrentLogFile is { } logFile)
        {
            Target(document, ns, QuillLogging.NormalTargetName)
                .SetAttributeValue(FileNameAttribute, logFile.Replace('\\', '/'));
        }
    }

    private static string? WriteTo(IEnumerable<string> targetNames)
    {
        var names = string.Join(",", targetNames);

        return string.IsNullOrEmpty(names) ? null : names;
    }

    private static XElement Rule(XDocument document, XNamespace ns, string ruleName) =>
        document.Descendants(ns + RuleElement)
            .FirstOrDefault(rule => (string?)rule.Attribute(RuleNameAttribute) == ruleName)
        ?? throw new InvalidOperationException(
            $"the configuration has no '{ruleName}' rule, so it cannot be updated here.");

    private static XElement Target(XDocument document, XNamespace ns, string targetName) =>
        document.Descendants(ns + TargetElement)
            .FirstOrDefault(target => (string?)target.Attribute(NameAttribute) == targetName)
        ?? throw new InvalidOperationException(
            $"the configuration has no '{targetName}' target, so it cannot be updated here.");

    private static void Write(string path, XDocument document)
    {
        var temporary = path + ".tmp";
        try
        {
            document.Save(temporary);

            Validate(temporary);

            File.Replace(temporary, path, path + ".bak");
        }
        finally
        {
            // best effort, as JsonConfigFileModifier does: a leftover .tmp must not fail the request
            try
            {
                File.Delete(temporary);
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }

    private static void Validate(string path)
    {
        var loaded = QuillLogging.Create(path);

        try
        {
            if (loaded.ConfigurationProblems.Count > 0)
                throw new InvalidOperationException(
                    "the rewritten configuration has settings the logging framework cannot apply: " +
                    string.Join("; ", loaded.ConfigurationProblems));
        }
        finally
        {
            loaded.Factory.Shutdown();
        }
    }
}
