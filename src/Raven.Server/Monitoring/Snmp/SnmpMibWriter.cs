using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Lextm.SharpSnmpLib;
using Raven.Client.Properties;

namespace Raven.Server.Monitoring.Snmp;

public class SnmpMibWriter : IAsyncDisposable
{
    private readonly HashSet<string> _seenObjects = new(StringComparer.OrdinalIgnoreCase);

    private readonly bool _includeServer;
    private readonly bool _includeCluster;
    private readonly bool _includeDatabases;
    private readonly StreamWriter _writer;

    public SnmpMibWriter([NotNull] Stream stream, bool includeServer, bool includeCluster, bool includeDatabases)
    {
        if (stream == null)
            throw new ArgumentNullException(nameof(stream));
        _includeServer = includeServer;
        _includeCluster = includeCluster;
        _includeDatabases = includeDatabases;

        _writer = new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false))
        {
            NewLine = "\n"
        };
    }

    public async Task WriteAsync()
    {
        var now = DateTime.UtcNow;

        await _writer.WriteLineAsync($"-- Copyright (C) {now.Year} Hibernating Rhinos, Ltd.  All Rights Reserved.");
        await _writer.WriteLineAsync();

        await _writer.WriteLineAsync("RAVENDB-MIB DEFINITIONS ::= BEGIN");
        await _writer.WriteLineAsync();

        await WriteImportsAsync();
        await WriteModuleAsync(now);

        await WriteObjectIdentifierAsync("hibernatingRhinos", "enterprises", "45751");
        await WriteObjectIdentifierAsync("server", "ravendb", "1");
        await WriteObjectIdentifierAsync("databases", "server", "5");


        if (_includeServer)
        {
            await SnmpOids.Server.WriteMibAsync(this);
            await _writer.WriteLineAsync();
        }

        if (_includeCluster)
        {
            await SnmpOids.Cluster.WriteMibAsync(this);
            await _writer.WriteLineAsync();
        }

        if (_includeDatabases)
        {
            await SnmpOids.Databases.General.WriteMibAsync(this);
            await _writer.WriteLineAsync();
        }

        await _writer.WriteLineAsync("END");
        await _writer.FlushAsync();
    }

    private async Task WriteSyntaxAsync(SnmpType typeCode)
    {
        switch (typeCode)
        {
            case SnmpType.Integer32:
                await _writer.WriteLineAsync("   SYNTAX Integer32");
                break;
            case SnmpType.OctetString:
                await _writer.WriteLineAsync("   SYNTAX DisplayString (SIZE (0..255))");
                break;
            case SnmpType.Gauge32:
                await _writer.WriteLineAsync("   SYNTAX Gauge32");
                break;
            case SnmpType.TimeTicks:
                await _writer.WriteLineAsync("   SYNTAX TimeTicks");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(typeCode), typeCode, null);
        }
    }

    private async Task WriteModuleAsync(DateTime now)
    {
        await _writer.WriteLineAsync("ravendb MODULE-IDENTITY");
        await _writer.WriteLineAsync($"    LAST-UPDATED \"{now:yyyyMMddHHmm}Z\"");
        await _writer.WriteLineAsync("    ORGANIZATION \"Hibernating Rhinos Ltd\"");
        await _writer.WriteLineAsync("    CONTACT-INFO \"https://ravendb.net/contact\"");
        await _writer.WriteLineAsync($"    DESCRIPTION \"MIB for the RavenDB {RavenVersionAttribute.Instance.FullVersion}\"");
        await _writer.WriteLineAsync($"    REVISION \"{now:yyyyMMddHHmm}Z\"");
        await _writer.WriteLineAsync($"    DESCRIPTION \"Generated MIB on {now:yyyyMMddHHmm} for RavenDB {RavenVersionAttribute.Instance.FullVersion}\"");
        await _writer.WriteLineAsync("    ::= { hibernatingRhinos 1 }");
        await _writer.WriteLineAsync();
    }

    private async Task WriteImportsAsync()
    {
        await _writer.WriteLineAsync("IMPORTS");
        await _writer.WriteLineAsync("   enterprises, MODULE-IDENTITY, Integer32, TimeTicks, Gauge32");
        await _writer.WriteLineAsync("      FROM SNMPv2-SMI");
        await _writer.WriteLineAsync("   OBJECT-TYPE");
        await _writer.WriteLineAsync("      FROM RFC1155-SMI");
        await _writer.WriteLineAsync("   DisplayString");
        await _writer.WriteLineAsync("      FROM SNMPv2-TC;");
        await _writer.WriteLineAsync();
    }

    private async Task WriteObjectIdentifierAsync(string objectName, string parentObjectName, string oid)
    {
        if (_seenObjects.Add(objectName) == false)
            throw new InvalidOperationException($"Already have seen object '{objectName}'.");

        objectName = objectName[0].ToString().ToLower() + objectName[1..];

        await _writer.WriteAsync(objectName);
        await WriteTabsAsync(4);
        await _writer.WriteAsync($"OBJECT IDENTIFIER ::= {{ {parentObjectName} {oid.Replace(".", " ")} }}");
        await _writer.WriteLineAsync();
        await _writer.WriteLineAsync();
    }

    public async Task WriteObjectAsync(string objectName, string parentObjectName, SnmpType typeCode, string description, string oid)
    {
        if (_seenObjects.Add(objectName) == false)
            throw new InvalidOperationException($"Already have seen object '{objectName}'.");

        objectName = objectName[0].ToString().ToLower() + objectName[1..];
        var finalOid = $"{parentObjectName} {oid.Replace(".", " ")}";

        await _writer.WriteLineAsync($"{objectName} OBJECT-TYPE");
        await WriteSyntaxAsync(typeCode);
        await _writer.WriteLineAsync("   MAX-ACCESS read-only");
        await _writer.WriteLineAsync("   STATUS current");
        await _writer.WriteLineAsync($"   DESCRIPTION \"{description}\"");
        await _writer.WriteLineAsync($"   ::= {{ {finalOid} }}");
        await _writer.WriteLineAsync();
    }

    private async Task WriteTabsAsync(int numberOfTabs = 1)
    {
        for (var i = 0; i < numberOfTabs; i++)
            await _writer.WriteAsync("\t");
    }

    public async ValueTask DisposeAsync()
    {
        if (_writer != null)
            await _writer.DisposeAsync();
    }
}
