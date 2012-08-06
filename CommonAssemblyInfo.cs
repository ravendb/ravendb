using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: AssemblyCompany("Hibernating Rhinos")]
[assembly: AssemblyCopyright("© Hibernating Rhinos 2004 - 2012. All rights reserved.")]
[assembly: AssemblyTrademark("")]
[assembly: ComVisible(false)]

[assembly: AssemblyVersion("1.2.0")]
[assembly: AssemblyFileVersion("1.2.13.0")]
[assembly: AssemblyInformationalVersion("1.2.0 / {commit}")]
[assembly: AssemblyProduct("RavenDB")]
[assembly: AssemblyDescription("A second generation LINQ enabled document database for .NET")]

#if SILVERLIGHT
[assembly: CLSCompliant(false)]
#else
[assembly: SuppressIldasm()]
[assembly: CLSCompliant(true)]
#endif

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: InternalsVisibleTo("Newtonsoft.Json.Dynamic, PublicKey=0024000004800000940000000602000000240000525341310004000001000100cbd8d53b9d7de30f1f1278f636ec462cf9c254991291e66ebb157a885638a517887633b898ccbcf0d5c5ff7be85a6abe9e765d0ac7cd33c68dac67e7e64530e8222101109f154ab14a941c490ac155cd1d4fcba0fabb49016b4ef28593b015cab5937da31172f03f67d09edda404b88a60023f062ae71d0b2e4438b74cc11dc9")]

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]