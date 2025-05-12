// -----------------------------------------------------------------------
//  <copyright file="CanAuthenticate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Routing;
using SlowTests.Authentication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Authentication
{
    public class AuthenticationStressTests : RavenTestBase
    {
        public AuthenticationStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Security)]
        public void Routes_Conventions()
        {
            foreach (var route in RouteScanner.AllRoutes.Values)
            {
                if (IsDatabaseRoute(route))
                {
                    AssertDatabaseRoute(route);
                    return;
                }

                AssertServerRoute(route);
            }

            static bool IsDatabaseRoute(RouteInformation route)
            {
                return route.Path.Contains("/databases/*/", StringComparison.OrdinalIgnoreCase);
            }

            static void AssertDatabaseRoute(RouteInformation route)
            {
                if (string.Equals(route.Method, "OPTIONS", StringComparison.OrdinalIgnoreCase) == false) // artificially added routes for CORS
                    Assert.True(RouteInformation.RouteType.Databases == route.TypeOfRoute, $"{route.Method} {route.Path} - {route.AuthorizationStatus}");

                Assert.True(route.AuthorizationStatus == AuthorizationStatus.ValidUser
                    || route.AuthorizationStatus == AuthorizationStatus.DatabaseAdmin, $"{route.Method} {route.Path} - {route.AuthorizationStatus}");
            }

            static void AssertServerRoute(RouteInformation route)
            {
                Assert.True(route.AuthorizationStatus == AuthorizationStatus.ValidUser
                    || route.AuthorizationStatus == AuthorizationStatus.ClusterAdmin
                    || route.AuthorizationStatus == AuthorizationStatus.Operator
                    || route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess
                    || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients, $"{route.Method} {route.Path} - {route.AuthorizationStatus}");
            }
        }


        [RavenFact(RavenTestCategory.Security)]
        public void Routes_Database_Read()
        {
            var settings = new Dictionary<string, string>();
            var certs = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var certificates = AuthenticationBasicTests.SetupServerAuthentication(Certificates, settings, certificates: certs);
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            using var server = GetNewServer(new ServerCreationOptions { CustomSettings = settings, RegisterForDisposal = true });
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: server);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [databaseName1] = DatabaseAccess.Read
            }, server: server);
            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => databaseName1,
                Server = server
            }))
            {
                using (var adminStore = new DocumentStore
                {
                    Urls = new[] { server.WebUrl },
                    Database = databaseName2,
                    Certificate = adminCert
                }.Initialize())
                {
                    adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                }

                var serverEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/admin/replication/conflicts/solver"),    // access handled internally
                    ("POST", "/setup/dns-n-cert"),                      // only available in setup mode
                    ("POST", "/setup/user-domains"),                    // only available in setup mode
                    ("POST", "/setup/populate-ips"),                    // only available in setup mode
                    ("GET", "/setup/parameters"),                       // only available in setup mode
                    ("GET", "/setup/ips"),                              // only available in setup mode
                    ("POST", "/setup/hosts"),                           // only available in setup mode
                    ("POST", "/setup/unsecured"),                       // only available in setup mode
                    ("POST", "/setup/unsecured/package"),               // only available in setup mode
                    ("POST", "/setup/continue/unsecured"),              // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                    ("GET", "/admin/debug/cluster-info-package"),       // heavy
                    ("GET", "/admin/debug/remote-cluster-info-package"),// heavy
                    ("GET", "/admin/debug/info-package"),               // heavy
                    ("GET", "/admin/debug/threads/contention"),         // heavy
                    ("GET", "/admin/debug/gcdump"),                     // heavy
                    ("GET", "/admin/debug/threads/stack-trace"),        // heavy
                    ("GET", "/admin/debug/memory/gc-events"),           // heavy
                    ("GET", "/admin/debug/memory/allocations"),         // heavy
                    ("GET", "/license/support"),                        // heavy
                    ("GET", "/admin/debug/threads/runaway"),            // heavy

                };

                var databaseEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/databases/*/admin/pull-replication/generate-certificate"), // heavy
                    ("POST", "/databases/*/studio/sample-data") // heavy
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler);
                    httpClient.BaseAddress = new Uri(server.WebUrl);

                    AssertServerRoutes(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;
                        if (route.EndpointType == EndpointType.Write)
                            canAccess = false;
                        else
                        {
                            canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser
                            || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients
                            || route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess;
                        }

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;
                        if (route.EndpointType == EndpointType.Write)
                            canAccess = false;
                        else
                        {
                            canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser;
                        }

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName2, httpClient, (route, statusCode) =>
                    {
                        var canAccess = false;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });
                }
            }
        }

        [RavenFact(RavenTestCategory.Security)]
        public void Routes_Database_ReadWrite()
        {
            var settings = new Dictionary<string, string>();
            var certs = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var certificates = AuthenticationBasicTests.SetupServerAuthentication(Certificates, settings, certificates: certs);
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            using var server = GetNewServer(new ServerCreationOptions { CustomSettings = settings, RegisterForDisposal = true });
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [databaseName1] = DatabaseAccess.ReadWrite
            }, server: server);
            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => databaseName1,
                Server = server
            }))
            {
                using (var adminStore = new DocumentStore
                {
                    Urls = new[] { server.WebUrl },
                    Database = databaseName2,
                    Certificate = adminCert
                }.Initialize())
                {
                    adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                }

                var serverEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/admin/replication/conflicts/solver"),    // access handled internally
                    ("POST", "/setup/dns-n-cert"),                      // only available in setup mode
                    ("POST", "/setup/user-domains"),                    // only available in setup mode
                    ("POST", "/setup/populate-ips"),                    // only available in setup mode
                    ("GET", "/setup/parameters"),                       // only available in setup mode
                    ("GET", "/setup/ips"),                              // only available in setup mode
                    ("POST", "/setup/hosts"),                           // only available in setup mode
                    ("POST", "/setup/unsecured"),                       // only available in setup mode
                    ("POST", "/setup/unsecured/package"),               // only available in setup mode
                    ("POST", "/setup/continue/unsecured"),              // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                    ("GET", "/admin/debug/cluster-info-package"),       // heavy
                    ("GET", "/admin/debug/remote-cluster-info-package"),// heavy
                    ("GET", "/admin/debug/info-package"),               // heavy
                    ("GET", "/admin/debug/threads/contention"),         // heavy
                    ("GET", "/admin/debug/gcdump"),                     // heavy
                    ("GET", "/admin/debug/threads/stack-trace"),        // heavy
                    ("GET", "/admin/debug/memory/gc-events"),           // heavy
                    ("GET", "/admin/debug/memory/allocations"),         // heavy
                    ("GET", "/license/support"),                        // heavy
                    ("GET", "/admin/debug/threads/runaway"),            // heavy
                };

                var databaseEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/databases/*/admin/pull-replication/generate-certificate"), // heavy
                    ("POST", "/databases/*/studio/sample-data") // heavy
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler);
                    httpClient.BaseAddress = new Uri(server.WebUrl);

                    AssertServerRoutes(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;
                        if (route.EndpointType == EndpointType.Write)
                            canAccess = false;
                        else
                        {
                            canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser
                            || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients
                            || route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess;
                        }

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName2, httpClient, (route, statusCode) =>
                    {
                        var canAccess = false;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });
                }
            }
        }

        [RavenFact(RavenTestCategory.Security)]
        public void Routes_Database_Admin()
        {
            var settings = new Dictionary<string, string>();
            var certs = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var certificates = AuthenticationBasicTests.SetupServerAuthentication(Certificates, settings, certificates: certs);
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            using var server = GetNewServer(new ServerCreationOptions { CustomSettings = settings, RegisterForDisposal = true });
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: server);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [databaseName1] = DatabaseAccess.Admin
            }, server: server);
            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => databaseName1,
                Server = server
            }))
            {
                using (var adminStore = new DocumentStore
                {
                    Urls = new[] { server.WebUrl },
                    Database = databaseName2,
                    Certificate = adminCert
                }.Initialize())
                {
                    adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                }

                var serverEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/admin/replication/conflicts/solver"),    // access handled internally
                    ("POST", "/setup/dns-n-cert"),                      // only available in setup mode
                    ("POST", "/setup/user-domains"),                    // only available in setup mode
                    ("POST", "/setup/populate-ips"),                    // only available in setup mode
                    ("GET", "/setup/parameters"),                       // only available in setup mode
                    ("GET", "/setup/ips"),                              // only available in setup mode
                    ("POST", "/setup/hosts"),                           // only available in setup mode
                    ("POST", "/setup/unsecured"),                       // only available in setup mode
                    ("POST", "/setup/unsecured/package"),               // only available in setup mode
                    ("POST", "/setup/continue/unsecured"),              // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                    ("GET", "/admin/debug/cluster-info-package"),       // heavy
                    ("GET", "/admin/debug/remote-cluster-info-package"),// heavy
                    ("GET", "/admin/debug/info-package"),               // heavy
                    ("GET", "/admin/debug/threads/contention"),         // heavy
                    ("GET", "/admin/debug/gcdump"),                     // heavy
                    ("GET", "/admin/debug/threads/stack-trace"),        // heavy
                    ("GET", "/admin/debug/memory/gc-events"),           // heavy
                    ("GET", "/admin/debug/memory/allocations"),         // heavy
                    ("GET", "/license/support"),                        // heavy
                    ("GET", "/admin/debug/threads/runaway"),            // heavy
                };

                var databaseEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/databases/*/admin/pull-replication/generate-certificate"), // heavy
                    ("POST", "/databases/*/studio/sample-data") // heavy
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler);
                    httpClient.BaseAddress = new Uri(server.WebUrl);

                    AssertServerRoutes(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;
                        if (route.EndpointType == EndpointType.Write)
                            canAccess = false;
                        else
                        {
                            canAccess = route.AuthorizationStatus == AuthorizationStatus.ValidUser
                            || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients
                            || route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess;
                        }

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName2, httpClient, (route, statusCode) =>
                    {
                        var canAccess = false;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });
                }
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Security, RavenArchitecture.AllX64)]
        public void Routes_Operator()
        {
            var settings = new Dictionary<string, string>();
            var certs = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var certificates = AuthenticationBasicTests.SetupServerAuthentication(Certificates, settings, certificates: certs);
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            using var server = GetNewServer(new ServerCreationOptions { CustomSettings = settings, RegisterForDisposal = true });
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: server);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.Operator, server: server);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => databaseName1,
                Server = server

            }))
            {
                using (var adminStore = new DocumentStore
                {
                    Urls = new[] { server.WebUrl },
                    Database = databaseName2,
                    Certificate = adminCert
                }.Initialize())
                {
                    adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                }

                var serverEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/admin/replication/conflicts/solver"),    // access handled internally
                    ("POST", "/setup/dns-n-cert"),                      // only available in setup mode
                    ("POST", "/setup/user-domains"),                    // only available in setup mode
                    ("POST", "/setup/populate-ips"),                    // only available in setup mode
                    ("GET", "/setup/parameters"),                       // only available in setup mode
                    ("GET", "/setup/ips"),                              // only available in setup mode
                    ("POST", "/setup/hosts"),                           // only available in setup mode
                    ("POST", "/setup/unsecured"),                       // only available in setup mode
                    ("POST", "/setup/unsecured/package"),               // only available in setup mode
                    ("POST", "/setup/continue/unsecured"),              // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                    ("GET", "/admin/debug/cluster-info-package"),       // heavy
                    ("GET", "/admin/debug/remote-cluster-info-package"),// heavy
                    ("GET", "/admin/debug/info-package"),               // heavy
                    ("GET", "/admin/debug/threads/contention"),         // heavy
                    ("GET", "/admin/debug/gcdump"),                     // heavy
                    ("GET", "/admin/debug/threads/stack-trace"),        // heavy
                    ("GET", "/admin/debug/memory/gc-events"),           // heavy
                    ("GET", "/admin/debug/memory/allocations"),         // heavy
                    ("GET", "/license/support"),                        // heavy 
                    ("GET", "/admin/debug/threads/runaway"),            // heavy
                };

                var databaseEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/databases/*/admin/pull-replication/generate-certificate"), // heavy
                    ("POST", "/databases/*/studio/sample-data") // heavy
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler);
                    httpClient.BaseAddress = new Uri(server.WebUrl);

                    AssertServerRoutes(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = route.AuthorizationStatus == AuthorizationStatus.Operator
                            || route.AuthorizationStatus == AuthorizationStatus.ValidUser
                            || route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients
                            || route.AuthorizationStatus == AuthorizationStatus.RestrictedAccess;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName2, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });
                }
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Certificates, RavenArchitecture.AllX64, NightlyBuildOnly = true)]
        public void Routes_ClusterAdmin()
        {
            var settings = new Dictionary<string, string>();
            var certs = Certificates.GenerateAndSaveSelfSignedCertificate(createNew: true);
            var certificates = AuthenticationBasicTests.SetupServerAuthentication(Certificates, settings, certificates: certs);
            var databaseName1 = GetDatabaseName();
            var databaseName2 = GetDatabaseName();
            using var server = GetNewServer(new ServerCreationOptions { CustomSettings = settings, RegisterForDisposal = true });
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: server);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: server);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = userCert,
                ModifyDatabaseName = s => databaseName1,
                Server = server
            }))
            {
                using (var adminStore = new DocumentStore
                {
                    Urls = new[] { server.WebUrl },
                    Database = databaseName2,
                    Certificate = adminCert
                }.Initialize())
                {
                    adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName2)));
                }

                var serverEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/admin/replication/conflicts/solver"),    // access handled internally
                    ("POST", "/setup/dns-n-cert"),                      // only available in setup mode
                    ("POST", "/setup/user-domains"),                    // only available in setup mode
                    ("POST", "/setup/populate-ips"),                    // only available in setup mode
                    ("GET", "/setup/parameters"),                       // only available in setup mode
                    ("GET", "/setup/ips"),                              // only available in setup mode
                    ("POST", "/setup/hosts"),                           // only available in setup mode
                    ("POST", "/setup/unsecured"),                       // only available in setup mode
                    ("POST", "/setup/unsecured/package"),               // only available in setup mode
                    ("POST", "/setup/continue/unsecured"),              // only available in setup mode
                    ("POST", "/setup/secured"),                         // only available in setup mode
                    ("GET", "/setup/letsencrypt/agreement"),            // only available in setup mode
                    ("POST", "/setup/letsencrypt"),                     // only available in setup mode
                    ("POST", "/setup/continue/extract"),                // only available in setup mode
                    ("POST", "/setup/continue"),                        // only available in setup mode
                    ("POST", "/setup/finish"),                          // only available in setup mode
                    ("POST", "/server/notification-center/dismiss"),    // access handled internally
                    ("POST", "/server/notification-center/postpone"),   // access handled internally
                    ("GET", "/admin/debug/cluster-info-package"),       // heavy
                    ("GET", "/admin/debug/remote-cluster-info-package"),// heavy
                    ("GET", "/admin/debug/info-package"),               // heavy
                };

                var databaseEndpointsToIgnore = new HashSet<(string Method, string Path)>
                {
                    ("POST", "/databases/*/admin/pull-replication/generate-certificate"), // heavy
                    ("POST", "/databases/*/studio/sample-data") // heavy
                };

                using (var httpClientHandler = new HttpClientHandler())
                {
                    httpClientHandler.ClientCertificates.Add(userCert);
                    httpClientHandler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;

                    var httpClient = new HttpClient(httpClientHandler);
                    httpClient.BaseAddress = new Uri(server.WebUrl);

                    AssertServerRoutes(RouteScanner.AllRoutes.Values, serverEndpointsToIgnore, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName1, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });

                    AssertDatabaseRoutes(RouteScanner.AllRoutes.Values, databaseEndpointsToIgnore, databaseName2, httpClient, (route, statusCode) =>
                    {
                        var canAccess = true;

                        var accessGiven = statusCode != HttpStatusCode.Forbidden;

                        if (canAccess != accessGiven)
                        {
                            throw new InvalidOperationException($"Wrong access on route '{route.Method} {route.Path}'. Should be '{canAccess}' but was '{accessGiven}'.");
                        }
                    });
                }
            }
        }

        private static void AssertServerRoutes(IEnumerable<RouteInformation> routes, HashSet<(string Method, string Path)> endpointsToIgnore, HttpClient httpClient, Action<RouteInformation, HttpStatusCode> assert)
        {
            foreach (var route in routes)
            {
                if (route.TypeOfRoute != RouteInformation.RouteType.None)
                    continue;

                if (route.Method == "OPTIONS")
                    continue; // artificially added routes for CORS

                if (endpointsToIgnore.Contains((route.Method, route.Path)))
                    continue;

                var requestUri = new Uri(route.Path, UriKind.Relative);

                HttpResponseMessage response;

                try
                {
                    response = httpClient.Send(new HttpRequestMessage { Method = new HttpMethod(route.Method), RequestUri = requestUri });
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Could not get response from {route.Method} '{requestUri}'.", e);
                }

                assert(route, response.StatusCode);
            }

        }

        private static void AssertDatabaseRoutes(IEnumerable<RouteInformation> routes, HashSet<(string Method, string Path)> endpointsToIgnore, string databaseName, HttpClient httpClient, Action<RouteInformation, HttpStatusCode> assert)
        {
            foreach (var route in routes)
            {
                if (route.TypeOfRoute != RouteInformation.RouteType.Databases)
                    continue;

                if (route.Method == "OPTIONS")
                    continue; // artificially added routes for CORS

                if (endpointsToIgnore.Contains((route.Method, route.Path)))
                    continue;

                var requestUri = new Uri(route.Path.Replace("/databases/*/", $"/databases/{databaseName}/", StringComparison.OrdinalIgnoreCase), UriKind.Relative);
                HttpResponseMessage response;
                try
                {
                    response = httpClient.Send(new HttpRequestMessage
                    {
                        Method = new HttpMethod(route.Method),
                        RequestUri = requestUri
                    });
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Could not get response from {route.Method} '{requestUri}'.", e);
                }

                assert(route, response.StatusCode);
            }
        }
    }
}
