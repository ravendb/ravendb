using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: AssemblyCompany("Hibernating Rhinos")]
[assembly: AssemblyCopyright("© Hibernating Rhinos 2004 - 2.13. All rights reserved.")]
[assembly: AssemblyTrademark("")]
[assembly: ComVisible(false)]

#if SILVERLIGHT
[assembly: AssemblyTitle("RavenDB (for Silverlight 5)")]
#elif NETFX_CORE
[assembly: AssemblyTitle("RavenDB (for WinRT)")]
#else
[assembly: AssemblyTitle("RavenDB (for .NET 4.5)")]
#endif

[assembly: AssemblyVersion("3.0.0")]
[assembly: AssemblyFileVersion("3.0.13.0")]
[assembly: AssemblyInformationalVersion("3.0.0 / 6dce79a")]
[assembly: AssemblyProduct("RavenDB")]
[assembly: AssemblyDescription("RavenDB is a second generation LINQ enabled document database for .NET")]

[assembly: InternalsVisibleTo("Newtonsoft.Json.Dynamic, PublicKey=0024000004800000940000000602000000240000525341310004000001000100cbd8d53b9d7de30f1f1278f636ec462cf9c254991291e66ebb157a885638a517887633b898ccbcf0d5c5ff7be85a6abe9e765d0ac7cd33c68dac67e7e64530e8222101109f154ab14a941c490ac155cd1d4fcba0fabb49016b4ef28593b015cab5937da31172f03f67d09edda404b88a60023f062ae71d0b2e4438b74cc11dc9")]

#if SILVERLIGHT
[assembly: CLSCompliant(false)]
#else
[assembly: CLSCompliant(true)]
#endif

#if !SILVERLIGHT && !NETFX_CORE
[assembly: SuppressIldasm()]
#endif

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]
