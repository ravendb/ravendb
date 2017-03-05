// -----------------------------------------------------------------------
//  <copyright file="AdminJsConsole.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Jint;
using Jint.Native;
using Jint.Parser;
using Jint.Runtime;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Rachis;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Json;
using Raven.Json.Linq;
using Voron;

namespace Raven.Database.JsConsole
{
    public class AdminJsConsole
    {
        private readonly DocumentDatabase database;
        private static readonly ILog _log = LogManager.GetCurrentClassLogger();
        public AdminJsConsole(DocumentDatabase database)
        {
            this.database = database;
        }

        public RavenJToken ApplyScript(AdminJsScript script)
        {
            Engine jintEngine;
            try
            {
                jintEngine = CreateEngine(script);
            }
            catch (NotSupportedException e)
            {
                throw new ParseException("Could not parse script", e);
            }
            catch (JavaScriptException e)
            {
                throw new ParseException("Could not parse script", e);
            }
            catch (Exception e)
            {
                throw new ParseException("Could not parse: " + Environment.NewLine + script.Script, e);
            }

            var result = jintEngine.Invoke("ExecuteAdminScript", database).ToObject();
            return RavenJToken.FromObject(result);
        }

        private Engine CreateEngine(AdminJsScript script)
        {
            var scriptWithProperLines = script.Script.NormalizeLineEnding();

            // NOTE: we merged few first lines of wrapping script to make sure {0} is at line 0.
            // This will all us to show proper line number using user lines locations.
            var wrapperScript = string.Format(@"function ExecuteAdminScript(databaseInner){{ var Raven = importNamespace('Raven'); return (function(database){{ {0} }}).apply(this, [databaseInner]); }};", scriptWithProperLines);
            using (LogContext.WithResource(database.Name))
            {
                _log.Info($"Excuting script from admin console:{Environment.NewLine}{wrapperScript}");
            }
                var jintEngine = new Engine(cfg =>
            {
#if DEBUG
                cfg.AllowDebuggerStatement();
#else
                cfg.AllowDebuggerStatement(false);
#endif                
                cfg.AllowClr(typeof(DocumentDatabase).Assembly/*Raven.Database*/, typeof(RavenJObject).Assembly/*Raven.Abstractions*/
                    ,typeof(Slice).Assembly/*Voron*/,typeof(Sparrow.Memory).Assembly/*Sparrow*/,typeof(RaftEngine).Assembly/*Rachis*/
                    ,typeof(Analyzer).Assembly/*Lucene*/, typeof(Term).Assembly/*Lucene*/, typeof(BooleanQuery).Assembly/*Lucene*/);
                /*bundles are dynamically loaded so we can't reference them unless they are loaded by MEF*/
                var bundles = AppDomain.CurrentDomain.GetAssemblies().Where(a=>a.FullName.StartsWith("Raven.Bundles."));
                cfg.AllowClr(bundles.ToArray());
                cfg.LimitRecursion(1024);
                cfg.MaxStatements(int.MaxValue);
            });

            jintEngine.Options.MaxStatements(10000);

            jintEngine.Execute(wrapperScript, new ParserOptions
            {
                Source = "main.js"
            });

            return jintEngine;
        }
    }
}
