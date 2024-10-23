using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.Properties;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Rachis.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class SetupHandler : ServerRequestHandler
    {
        [RavenAction("/setup/alive", "GET", AuthorizationStatus.UnauthenticatedClients, CorsMode = CorsMode.Public)]
        public Task ServerAlive()
        {
            return NoContent();
        }

        [RavenAction("/setup/dns-n-cert", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task DnsCertBridge()
        {
            AssertOnlyInSetupMode();
            var action = GetQueryStringValueAndAssertIfSingleAndNotEmpty("action"); // Action can be: claim | user-domains | check-availability

            using (var reader = new StreamReader(RequestBodyStream()))
            {
                var payload = await reader.ReadToEndAsync();
                var content = new StringContent(payload, Encoding.UTF8, "application/json");

                try
                {
                    string error = null;
                    object result = null;
                    string responseString = null;
                    string errorMessage = null;

                    try
                    {
                        var response = await ApiHttpClient.Instance.PostAsync("/api/v1/dns-n-cert/" + action, content).ConfigureAwait(false);

                        HttpContext.Response.StatusCode = (int)response.StatusCode;
                        responseString = await response.Content.ReadAsStringWithZstdSupportAsync().ConfigureAwait(false);

                        if ((int)response.StatusCode >= 500 && (int)response.StatusCode <= 599)
                        {
                            error = responseString;
                            errorMessage = GeneralDomainRegistrationError;
                        }
                        else
                        {
                            result = JsonConvert.DeserializeObject<JObject>(responseString);
                            if (result != null)
                            {
                                if (((JObject)result).TryGetValue(nameof(ExceptionDispatcher.ExceptionSchema.Error), out var err))
                                    error = err.ToString();

                                if (((JObject)result).TryGetValue(nameof(ExceptionDispatcher.ExceptionSchema.Message), out var msg))
                                    errorMessage = msg.ToString();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        result = responseString;
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                        error = e.ToString();
                        errorMessage = DomainRegistrationServiceUnreachableError;
                    }

                    await using (var streamWriter = new StreamWriter(ResponseBodyStream()))
                    {
                        if (error != null)
                        {
                            new JsonSerializer().Serialize(streamWriter, new
                            {
                                Message = errorMessage,
                                Response = result,
                                Error = error,
                                Type = typeof(RavenException).FullName
                            });

                            await streamWriter.FlushAsync();
                        }
                        else
                        {
                            await streamWriter.WriteAsync(responseString);
                        }

                        await streamWriter.FlushAsync();
                    }
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(GeneralDomainRegistrationError, e);
                }
            }
        }

        [RavenAction("/setup/user-domains", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task UserDomains()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "license activation");
                var licenseInfo = JsonDeserializationServer.LicenseInfo(json);

                var licenseStatus = LicenseManager.GetLicenseStatus(licenseInfo.License);
                if (licenseStatus.Version.Major < 6)
                {
                    throw new LicenseLimitException(
                        $"Your license ('{licenseStatus.Id}') version '{licenseStatus.Version}' doesn't allow you to set up a server of version '{RavenVersionAttribute.Instance.FullVersion}'. " +
                        $"Please proceed to the https://ravendb.net/l/8O2YU1 website to perform the license upgrade first.");
                }

                var content = new StringContent(JsonConvert.SerializeObject(licenseInfo), Encoding.UTF8, "application/json");
                try
                {
                    string error = null;
                    object result = null;
                    string responseString = null;
                    string errorMessage = null;

                    try
                    {
                        var response = await ApiHttpClient.Instance.PostAsync("/api/v1/dns-n-cert/user-domains", content).ConfigureAwait(false);

                        HttpContext.Response.StatusCode = (int)response.StatusCode;
                        responseString = await response.Content.ReadAsStringWithZstdSupportAsync().ConfigureAwait(false);

                        if (response.IsSuccessStatusCode == false)
                        {
                            error = responseString;
                            errorMessage = GeneralDomainRegistrationError;
                        }
                        else
                        {
                            result = JsonConvert.DeserializeObject<JObject>(responseString);
                        }
                    }
                    catch (Exception e)
                    {
                        result = responseString;
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                        error = e.ToString();
                        errorMessage = DomainRegistrationServiceUnreachableError;
                    }

                    if (error != null)
                    {
                        JToken errorJToken = null;
                        if (responseString != null)
                        {
                            JsonConvert.DeserializeObject<JObject>(responseString).TryGetValue("Error", out errorJToken);
                        }

                        await using (var streamWriter = new StreamWriter(ResponseBodyStream()))
                        {
                            new JsonSerializer().Serialize(streamWriter, new
                            {
                                Message = errorMessage,
                                Response = result,
                                Error = errorJToken ?? error
                            });

                            await streamWriter.FlushAsync();
                        }

                        return;
                    }

                    var results = JsonConvert.DeserializeObject<UserDomainsResult>(responseString);

                    var fullResult = new UserDomainsAndLicenseInfo
                    {
                        UserDomainsWithIps = new UserDomainsWithIps
                        {
                            Emails = results.Emails,
                            RootDomains = results.RootDomains,
                            Domains = new Dictionary<string, List<SubDomainAndIps>>()
                        }
                    };

                    foreach (var domain in results.Domains)
                    {
                        var list = new List<SubDomainAndIps>();
                        foreach (var subDomain in domain.Value)
                        {
                            try
                            {
                                list.Add(new SubDomainAndIps
                                {
                                    SubDomain = subDomain,
                                    // The ip list will be populated on the next call (/setup/populate-ips), when we know which root domain the user selected
                                });
                            }
                            catch (Exception)
                            {
                                continue;
                            }
                        }

                        fullResult.UserDomainsWithIps.Domains.Add(domain.Key, list);
                    }

                    licenseStatus = await SetupManager
                        .GetUpdatedLicenseStatus(ServerStore, licenseInfo.License)
                        .ConfigureAwait(false);
                    fullResult.MaxClusterSize = licenseStatus.MaxClusterSize;
                    fullResult.LicenseType = licenseStatus.Type;

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        var blittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(fullResult, context);
                        context.Write(writer, blittable);
                    }
                }
                catch (LicenseExpiredException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(GeneralDomainRegistrationError, e);
                }
            }
        }

        [RavenAction("/setup/populate-ips", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task PopulateIps()
        {
            AssertOnlyInSetupMode();
            var rootDomain = GetQueryStringValueAndAssertIfSingleAndNotEmpty("rootDomain");

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var userDomainsWithIpsJson = await context.ReadForMemoryAsync(RequestBodyStream(), "setup-secured"))
            {
                var userDomainsWithIps = JsonDeserializationServer.UserDomainsWithIps(userDomainsWithIpsJson);

                foreach (var domain in userDomainsWithIps.Domains)
                {
                    foreach (var subDomain in domain.Value)
                    {
                        try
                        {
                            subDomain.Ips = (await Dns.GetHostAddressesAsync(subDomain.SubDomain + "." + rootDomain)).Select(ip => ip.ToString()).ToList();
                        }
                        catch (Exception)
                        {
                            continue;
                        }
                    }
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var blittable = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(userDomainsWithIps, context);
                    context.Write(writer, blittable);
                }
            }
        }

        [RavenAction("/setup/parameters", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public async Task GetSetupParameters()
        {
            AssertOnlyInSetupMode();
            var setupParameters = await SetupParameters.Get(ServerStore);
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(SetupParameters.FixedServerPortNumber));

                if (setupParameters.FixedServerPortNumber.HasValue)
                {
                    writer.WriteInteger(setupParameters.FixedServerPortNumber.Value);
                }
                else
                {
                    writer.WriteNull();
                }

                writer.WriteComma();

                writer.WritePropertyName(nameof(SetupParameters.IsDocker));
                writer.WriteBool(setupParameters.IsDocker);
                writer.WriteComma();

                writer.WritePropertyName(nameof(SetupParameters.IsAzure));
                writer.WriteBool(setupParameters.IsAzure);
                writer.WriteComma();

                writer.WritePropertyName(nameof(SetupParameters.IsAws));
                writer.WriteBool(setupParameters.IsAws);
                writer.WriteComma();

                writer.WritePropertyName(nameof(SetupParameters.RunningOnPosix));
                writer.WriteBool(setupParameters.RunningOnPosix);
                writer.WriteComma();

                writer.WritePropertyName(nameof(SetupParameters.RunningOnMacOsx));
                writer.WriteBool(setupParameters.RunningOnMacOsx);

                writer.WriteEndObject();
            }
        }

        [RavenAction("/setup/ips", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public async Task GetIps()
        {
            AssertOnlyInSetupMode();

            NetworkInterface[] netInterfaces = null;
            try
            {
                netInterfaces = NetworkInterface.GetAllNetworkInterfaces();
            }
            catch (Exception)
            {
                // https://github.com/dotnet/corefx/issues/26476
                // If GetAllNetworkInterfaces is not supported, we'll just return the default: 127.0.0.1
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var setupParameters = await SetupParameters.Get(ServerStore);

                writer.WriteStartObject();
                writer.WritePropertyName("MachineName");
                writer.WriteString(Environment.MachineName);
                writer.WriteComma();
                writer.WritePropertyName("NetworkInterfaces");
                writer.WriteStartArray();
                var first = true;

                List<string> ips;
                if (netInterfaces != null)
                {
                    foreach (var netInterface in netInterfaces)
                    {
                        ips = netInterface.GetIPProperties().UnicastAddresses
                            .Where(x =>
                            {
                                // filter 169.254.xxx.xxx out, they are not meaningful for binding
                                if (x.Address.AddressFamily != AddressFamily.InterNetwork)
                                    return false;
                                var addressBytes = x.Address.GetAddressBytes();

                                // filter 127.xxx.xxx.xxx out, in docker only
                                if (setupParameters.IsDocker && addressBytes[0] == 127)
                                    return false;

                                return addressBytes[0] != 169 || addressBytes[1] != 254;
                            })
                            .Select(addr => addr.Address.ToString())
                            .ToList();

                        // If there's a hostname in the server url, add it to the list
                        if (setupParameters.DockerHostname != null && ips.Contains(setupParameters.DockerHostname) == false)
                            ips.Add(setupParameters.DockerHostname);

                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        writer.WriteStartObject();
                        writer.WritePropertyName("Name");
                        writer.WriteString(netInterface.Name);
                        writer.WriteComma();
                        writer.WritePropertyName("Description");
                        writer.WriteString(netInterface.Description);
                        writer.WriteComma();
                        writer.WriteArray("Addresses", ips);
                        writer.WriteEndObject();
                    }
                }
                else
                {
                    // https://github.com/dotnet/corefx/issues/26476
                    // If GetAllNetworkInterfaces is not supported, we'll just return the default: 127.0.0.1
                    ips = new List<string>
                    {
                        "127.0.0.1"
                    };
                    writer.WriteStartObject();
                    writer.WritePropertyName("Name");
                    writer.WriteString("Loopback Interface");
                    writer.WriteComma();
                    writer.WritePropertyName("Description");
                    writer.WriteString("Loopback Interface");
                    writer.WriteComma();
                    writer.WriteArray("Addresses", ips);
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }
        }

        [RavenAction("/setup/hosts", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task GetHosts()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var certificateJson = await context.ReadForMemoryAsync(RequestBodyStream(), "setup-certificate"))
            {
                var certDef = JsonDeserializationServer.CertificateDefinition(certificateJson);

                X509Certificate2 certificate = null;
                string cn;

                try
                {
                    certificate = CertificateLoaderUtil.CreateCertificateFromPfx(Convert.FromBase64String(certDef.Certificate), certDef.Password);

                    cn = certificate.GetNameInfo(X509NameType.SimpleName, false);
                }
                catch (Exception e)
                {
                    throw new BadRequestException($"Failed to extract the CN property from the certificate {certificate?.FriendlyName}. Maybe the password is wrong?", e);
                }

                if (cn == null)
                {
                    throw new BadRequestException($"Failed to extract the CN property from the certificate. CN is null");
                }

                if (cn.LastIndexOf('*') > 0)
                {
                    throw new NotSupportedException("The wildcard CN name contains a '*' which is not at the first character of the string. It is not supported in the Setup Wizard, you can do a manual setup instead.");
                }

                try
                {
                    SecretProtection.ValidateKeyUsages("Setup Wizard", certificate, ServerStore.Configuration.Security.CertificateValidationKeyUsages);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to load the uploaded certificate. Did you accidentally upload a client certificate?", e);
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("CN");
                    writer.WriteString(cn);
                    writer.WriteComma();
                    writer.WritePropertyName("AlternativeNames");
                    writer.WriteStartArray();

                    var first = true;
                    foreach (var value in CertificateUtils.GetCertificateAlternativeNames(certificate))
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        writer.WriteString(value);
                    }

                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }
            }
        }
        
        [RavenAction("/setup/unsecured/package", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupUnsecuredPackage()
        {
            AssertOnlyInSetupMode();

            var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();
            var operationCancelToken = CreateHttpRequestBoundOperationToken();
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (var setupInfoJson = await context.ReadForMemoryAsync(stream, "setup-unsecured"))
            {
                // Making sure we don't have leftovers from previous setup
                try
                {
                    var command = new DeleteTopologyCommand(ServerStore.Engine);
                    await ServerStore.Engine.TxMerger.Enqueue(command);
                }
                catch (Exception)
                {
                    // ignored
                }

                var unsecuredSetupInfo = JsonDeserializationServer.UnsecuredSetupInfo(setupInfoJson);

                var operationResult = await ServerStore.Operations.AddLocalOperation(
                    operationId.Value,
                    OperationType.Setup,
                    "Setting up RavenDB in unsecured mode.",
                    detailedDescription: null,
                    progress => SetupManager.SetupUnsecuredTask(progress,
                        unsecuredSetupInfo,
                        ServerStore,
                        operationCancelToken.Token),
                    token: operationCancelToken);

                // unsecured ->  toggle off(no zip only) -> single node  =>> don't create zip
                if (unsecuredSetupInfo.ZipOnly == false && unsecuredSetupInfo.NodeSetupInfos.Count == 1)
                    return;

                var zip = ((SetupProgressAndResult)operationResult).SettingsZipFile;

                var fileName = $"Unsecure.Cluster.Settings {DateTime.UtcNow:yyyy-MM-dd HH-mm}.zip ";
                var contentDisposition = $"attachment; filename={fileName}";

                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "application/octet-stream";

                await HttpContext.Response.Body.WriteAsync(zip, 0, zip.Length);
            }
        }

        [RavenAction("/setup/secured", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupSecured()
        {
            AssertOnlyInSetupMode();

            var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();
            var operationCancelToken = CreateHttpRequestBoundOperationToken();
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = await context.ReadForMemoryAsync(stream, "setup-secured"))
            {
                var setupInfo = JsonDeserializationServer.SetupInfo(setupInfoJson);

                var operationResult = await ServerStore.Operations.AddLocalOperation(
                    operationId.Value,
                    OperationType.Setup,
                    "Setting up RavenDB in secured mode.",
                    detailedDescription: null,
                    progress => SetupManager.SetupSecuredTask(progress, setupInfo, ServerStore, operationCancelToken.Token),
                    token: operationCancelToken);

                var zip = ((SetupProgressAndResult)operationResult).SettingsZipFile;

                var nodeCert = CertificateLoaderUtil.CreateCertificateFromPfx(Convert.FromBase64String(setupInfo.Certificate), setupInfo.Password);

                var cn = nodeCert.GetNameInfo(X509NameType.SimpleName, false);

                var fileName = $"{cn}.Cluster.Settings {DateTime.UtcNow:yyyy-MM-dd HH-mm}.zip ";
                var contentDisposition = $"attachment; filename={fileName}";
                
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "application/octet-stream";

                await HttpContext.Response.Body.WriteAsync(zip, 0, zip.Length);
            }
        }

        [RavenAction("/setup/letsencrypt/agreement", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupAgreement()
        {
            AssertOnlyInSetupMode();

            var email = GetQueryStringValueAndAssertIfSingleAndNotEmpty("email");

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var baseUri = new Uri("https://letsencrypt.org/");
                var uri = new Uri(baseUri, await SetupManager.LetsEncryptAgreement(email, ServerStore));

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Uri");
                    writer.WriteString(uri.AbsoluteUri);
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/setup/letsencrypt", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupLetsEncrypt()
        {
            AssertOnlyInSetupMode();

            var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();

            var operationCancelToken = CreateHttpRequestBoundOperationToken();
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = await context.ReadForMemoryAsync(stream, "setup-lets-encrypt"))
            {
                var setupInfo = JsonDeserializationServer.SetupInfo(setupInfoJson);

                var operationResult = await ServerStore.Operations.AddLocalOperation(
                    operationId.Value,
                    OperationType.Setup,
                    "Setting up RavenDB with a Let's Encrypt certificate",
                    detailedDescription: null,
                    progress => SetupManager.SetupLetsEncryptTask(progress, setupInfo, ServerStore, operationCancelToken.Token),
                    token: operationCancelToken);

                var zip = ((SetupProgressAndResult)operationResult).SettingsZipFile;

                var fileName = $"{setupInfo.Domain}.Cluster.Settings {DateTime.UtcNow:yyyy-MM-dd HH-mm}.zip ";
                var contentDisposition = $"attachment; filename={fileName}";
                
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "application/octet-stream";

                await HttpContext.Response.Body.WriteAsync(zip, 0, zip.Length);
            }
        }

        [RavenAction("/setup/continue/extract", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task ExtractInfoFromZip()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var continueSetupInfoJson = await context.ReadForMemoryAsync(RequestBodyStream(), "continue-setup-info"))
            {
                var continueSetupInfo = JsonDeserializationServer.ContinueSetupInfo(continueSetupInfoJson);
                byte[] zipBytes;
                try
                {
                    zipBytes = Convert.FromBase64String(continueSetupInfo.Zip);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Unable to parse the {nameof(continueSetupInfo.Zip)} property, expected a Base64 value", e);
                }

                try
                {
                    var urlOrIpByTag = new Dictionary<string, string>();
                    await using (var ms = new MemoryStream(zipBytes))
                    using (var archive = new ZipArchive(ms, ZipArchiveMode.Read, false))
                    {
                        foreach (var entry in archive.Entries)
                        {
                            if (entry.Name.Equals("settings.json") == false)
                                continue;

                            var tag = entry.FullName.Substring(0, entry.FullName.Length - "/settings.json".Length);

                            using (var settingsJson = await context.ReadForMemoryAsync(entry.Open(), "settings-json"))
                                if (settingsJson.TryGet(nameof(ConfigurationNodeInfo.PublicServerUrl), out string publicServerUrl))
                                {
                                    urlOrIpByTag[tag] = publicServerUrl;
                                }
                                else if (settingsJson.TryGet(nameof(ConfigurationNodeInfo.ServerUrl), out string serverUrl))
                                {
                                    urlOrIpByTag[tag] = serverUrl;
                                }
                        }
                    }
    
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartArray();
                        var first = true;

                        foreach (var node in urlOrIpByTag)
                        {
                            if (first == false)
                                writer.WriteComma();

                            writer.WriteStartObject();
                            writer.WritePropertyName(nameof(ConfigurationNodeInfo.Tag));
                            writer.WriteString(node.Key);
                            writer.WriteComma();
                            var uri = new Uri(node.Value);
                            if (uri.Scheme.Equals(Uri.UriSchemeHttp) == false)
                            {
                                writer.WritePropertyName(nameof(ConfigurationNodeInfo.PublicServerUrl));
                                writer.WriteString(node.Value);
                                writer.WriteEndObject();
                            }
                            else
                            {
                                writer.WritePropertyName(nameof(ConfigurationNodeInfo.ServerUrl));
                                writer.WriteString(node.Value);
                                writer.WriteEndObject();
                            }
                            
                            first = false;
                        }

                        writer.WriteEndArray();
                    }
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
                }
            }
        }

        [RavenAction("/setup/continue/unsecured", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task ContinueUnsecuredClusterSetup()
        {
            AssertOnlyInSetupMode();

            var operationCancelToken = CreateHttpRequestBoundOperationToken();
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var continueSetupInfoJson = await context.ReadForMemoryAsync(RequestBodyStream(), "continue-cluster-setup"))
            {
                var continueSetupInfo = JsonDeserializationServer.ContinueSetupInfo(continueSetupInfoJson);

                await ServerStore.Operations.AddLocalOperation(
                    operationId.Value,
                    OperationType.Setup,
                    "Continue Unsecured Cluster Setup.",
                    detailedDescription: null,
                    progress => SetupManager.ContinueUnsecuredClusterSetupTask(progress, continueSetupInfo, ServerStore, operationCancelToken.Token),
                    token: operationCancelToken);
            }

            NoContentStatus();
        }

        
        
        [RavenAction("/setup/continue", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task ContinueClusterSetup()
        {
            AssertOnlyInSetupMode();

            var operationCancelToken = CreateHttpRequestBoundOperationToken();
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var continueSetupInfoJson = await context.ReadForMemoryAsync(RequestBodyStream(), "continue-cluster-setup"))
            {
                var continueSetupInfo = JsonDeserializationServer.ContinueSetupInfo(continueSetupInfoJson);

                await ServerStore.Operations.AddLocalOperation(
                    operationId.Value,
                    OperationType.Setup,
                    "Continue Cluster Setup.",
                    detailedDescription: null,
                    progress => SetupManager.ContinueClusterSetupTask(progress, continueSetupInfo, ServerStore, operationCancelToken.Token),
                    token: operationCancelToken);
            }

            NoContentStatus();
        }

        [RavenAction("/setup/finish", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public Task SetupFinish()
        {
            AssertOnlyInSetupMode();

            Task.Run(async () =>
            {
                // we want to give the studio enough time to actually
                // get a valid response from the server before we reset
                await Task.Delay(250);

                Program.RestartServer();
            });

            return NoContent();
        }

        private void AssertOnlyInSetupMode()
        {
            if (ServerStore.Configuration.Core.SetupMode == SetupMode.Initial)
                return;

            throw new AuthorizationException("RavenDB has already been setup. Cannot use the /setup endpoints any longer.");
        }

        private static string GeneralDomainRegistrationError = "Registration error.";
        private static string DomainRegistrationServiceUnreachableError = $"Failed to contact {ApiHttpClient.ApiRavenDbNet}. Please try again later.";
    }

    public sealed class LicenseInfo
    {
        public License License { get; set; }
    }

    public sealed class ConfigurationNodeInfo
    {
        public string Tag { get; set; }
        public string ServerUrl { get; set; }
        public string PublicServerUrl { get; set; }
    }
}
