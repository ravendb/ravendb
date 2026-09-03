using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Server.Commercial;
using Raven.Server.Commercial.LetsEncrypt;

namespace rvn
{
    internal static class DnsRecordUpdate
    {
        public static async Task RunAsync(
            string licensePath,
            string fullDomain,
            IReadOnlyList<string> nodeSpecs,
            bool registerTcpDnsRecords,
            CancellationToken token)
        {
            var license = ReadLicense(licensePath);

            fullDomain = fullDomain.Trim().ToLowerInvariant();
            var firstDot = fullDomain.IndexOf('.');
            if (firstDot <= 0 || firstDot == fullDomain.Length - 1)
                throw new InvalidOperationException($"Invalid domain '{fullDomain}'. Expected a full domain such as 'mycompany.development.run'.");

            var domain = fullDomain.Substring(0, firstDot);
            var rootDomain = fullDomain.Substring(firstDot + 1);

            var nodeSetupInfos = ParseNodes(nodeSpecs);

            var setupInfo = new SetupInfo
            {
                License = license,
                Domain = domain,
                RootDomain = rootDomain,
                LocalNodeTag = null,
                NodeSetupInfos = nodeSetupInfos
            };

            var progress = new SetupProgressAndResult(tuple =>
            {
                if (tuple.Message != null)
                    Console.WriteLine(tuple.Message);

                if (tuple.Exception != null)
                    Console.Error.WriteLine(tuple.Exception);
            }, SetupMode.None);

            await RavenDnsRecordHelper.UpdateDnsRecordsTask(new UpdateDnsRecordParameters
            {
                Challenge = null,
                SetupInfo = setupInfo,
                Progress = progress,
                RegisterTcpDnsRecords = registerTcpDnsRecords,
                Token = token
            });

            Console.WriteLine($"Successfully registered DNS record(s) for '{fullDomain}' in api.ravendb.net.");
        }

        private static License ReadLicense(string licensePath)
        {
            if (File.Exists(licensePath) == false)
                throw new InvalidOperationException($"License file was not found at '{Path.GetFullPath(licensePath)}'.");

            License license;
            try
            {
                license = JsonConvert.DeserializeObject<License>(File.ReadAllText(licensePath));
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to read the license file at '{Path.GetFullPath(licensePath)}'. It is not valid license JSON: {e.Message}", e);
            }

            return license ?? throw new InvalidOperationException($"The license file at '{Path.GetFullPath(licensePath)}' is empty or not a valid license.");
        }

        private static Dictionary<string, NodeInfo> ParseNodes(IReadOnlyList<string> nodeSpecs)
        {
            if (nodeSpecs == null || nodeSpecs.Count == 0)
                throw new InvalidOperationException("At least one record must be provided using -n|--node <ip>[,<ip>...]=<subdomain>[,<subdomain>...].");

            var nodes = new Dictionary<string, NodeInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var spec in nodeSpecs)
            {
                var separatorIndex = spec.IndexOf('=');
                if (separatorIndex <= 0)
                    throw new InvalidOperationException(
                        $"Invalid record '{spec}'. Expected format is <ip>[,<ip>...]=<subdomain>[,<subdomain>...], for example 10.0.0.1=a, 10.0.0.2=dashboard,db or 10.0.0.3,2001:db8::1=web.");

                var ipsPart = spec.Substring(0, separatorIndex).Trim();
                var subdomainsPart = spec.Substring(separatorIndex + 1).Trim();

                var addresses = new List<string>();
                foreach (var rawIp in ipsPart.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                {
                    if (IPAddress.TryParse(rawIp, out var parsed) == false)
                        throw new InvalidOperationException($"Invalid IP address '{rawIp}' in '{spec}'.");

                    addresses.Add(parsed.ToString());
                }

                if (addresses.Count == 0)
                    throw new InvalidOperationException(
                        $"No IP address was provided in '{spec}'. Expected format is <ip>[,<ip>...]=<subdomain>[,<subdomain>...].");

                var subdomains = subdomainsPart.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                if (subdomains.Length == 0)
                    throw new InvalidOperationException(
                        $"No subdomain was provided in '{spec}'. Expected format is <ip>[,<ip>...]=<subdomain>[,<subdomain>...].");

                foreach (var rawSubdomain in subdomains)
                {
                    var subdomain = rawSubdomain.ToLowerInvariant();
                    if (nodes.ContainsKey(subdomain))
                        throw new InvalidOperationException($"Subdomain '{subdomain}' was specified more than once.");

                    nodes[subdomain] = new NodeInfo
                    {
                        Addresses = new List<string>(addresses)
                    };
                }
            }

            return nodes;
        }
    }
}
