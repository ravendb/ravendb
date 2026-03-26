using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using FastTests;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;
using Lextm.SharpSnmpLib.Security;
using Raven.Server.Config;
using Raven.Server.Monitoring.Snmp;
using Raven.Server.Monitoring.Snmp.Providers;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25172 : RavenTestBase
{
    public RavenDB_25172(ITestOutputHelper output) : base(output)
    {
    }

    public static IEnumerable<object[]> V3SecurityCombinations()
    {
        foreach (var authProtocol in (SnmpAuthenticationProtocol[])Enum.GetValues(typeof(SnmpAuthenticationProtocol)))
        {
            foreach (var privProtocol in (SnmpPrivacyProtocol[])Enum.GetValues(typeof(SnmpPrivacyProtocol)))
            {
                yield return new object[] { authProtocol, privProtocol };
            }
        }
    }

    [RavenTheory(RavenTestCategory.Monitoring)]
    [MemberData(nameof(V3SecurityCombinations))]
    public void CanGetSnmp_V3_WithAuthentication(SnmpAuthenticationProtocol authProtocol, SnmpPrivacyProtocol privProtocol)
    {
        var port = ReservePort().Port;
        const string testAuthPass = "test-auth-pass";
        const string testPrivPass = "test-priv-pass";
        const string userName = "ravendb";

        var customSettings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Enabled)] = "true",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.SupportedVersions)] = "V3",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Port)] = port.ToString(),
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.AuthenticationProtocol)] = authProtocol.ToString(),
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.AuthenticationPassword)] = testAuthPass,
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.PrivacyProtocol)] = privProtocol.ToString(),
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.PrivacyPassword)] = testPrivPass,
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.AuthenticationUser)] = userName,
        };

        UseNewLocalServer(customSettings);

        // Bootstrap the cluster - needed for the snmp to be available.
        using (GetDocumentStore(new Options { CreateDatabase = true }))
        {
        }

        var ip = new Uri(Server.WebUrl).Host;
        var endpoint = new IPEndPoint(IPAddress.Parse(ip), port);

        ReportMessage report = null;
        // Might take a while for the snmp to be available.
        for (var i = 0; i < 15; i++)
        {
            try
            {
                var discovery = Messenger.GetNextDiscovery(SnmpType.GetRequestPdu);
                report = discovery.GetResponse(500, endpoint);
                if (report != null)
                    break;
            }
            catch (Exception)
            {
                Thread.Sleep(125);
            }
        }

        Assert.NotNull(report);

        IAuthenticationProvider authProvider = authProtocol switch
        {
            SnmpAuthenticationProtocol.MD5 => new MD5AuthenticationProvider(new OctetString(testAuthPass)),
            SnmpAuthenticationProtocol.SHA1 => new SHA1AuthenticationProvider(new OctetString(testAuthPass)),
            _ => throw new ArgumentOutOfRangeException(nameof(authProtocol), authProtocol, null)
        };

        IPrivacyProvider privProvider = privProtocol switch
        {
            SnmpPrivacyProtocol.None => DefaultPrivacyProvider.DefaultPair,
            SnmpPrivacyProtocol.DES => new DotnetDESPrivacyProvider(new OctetString(testPrivPass), authProvider),
            SnmpPrivacyProtocol.AES => new DotnetAESPrivacyProvider(new OctetString(testPrivPass), authProvider),
            _ => throw new ArgumentOutOfRangeException(nameof(privProtocol), privProtocol, null)
        };

        var request = new GetRequestMessage(VersionCode.V3, Messenger.NextMessageId, Messenger.NextRequestId,
            new OctetString(userName),
            [new Variable(new ObjectIdentifier("1.3.6.1.4.1.45751.1.1.1.1.5"))],
            privProvider,
            Messenger.MaxMessageSize,
            report);

        ISnmpMessage reply = request.GetResponse(500, endpoint);

        Assert.NotNull(reply);
        Assert.Equal(0, reply.Pdu().ErrorStatus.ToInt32());
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public void CanGetSnmp_V2C()
    {
        var port = ReservePort().Port;
        var communityString = "public-test";
        var customSettings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Enabled)] = "true",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.SupportedVersions)] = "V2C",
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Port)] = port.ToString(),
            [RavenConfiguration.GetKey(x => x.Monitoring.Snmp.Community)] = communityString
        };

        UseNewLocalServer(customSettings);

        using (GetDocumentStore(new Options { CreateDatabase = true }))
        {
        }

        var ip = new Uri(Server.WebUrl).Host;
        var endpoint = new IPEndPoint(IPAddress.Parse(ip), port);

        // For v2c, there is no discovery. We just send the request directly.
        for (var i = 0; i < 15; i++)
        {
            try
            {
                var result = Messenger.Get(VersionCode.V2,
                    endpoint,
                    new OctetString(communityString),
                    [new Variable(new ObjectIdentifier("1.3.6.1.4.1.45751.1.1.1.1.5"))],
                    100);

                Assert.NotEmpty(result);
            }
            catch (Exception)
            {
                Thread.Sleep(500);
            }
        }
    }
}
