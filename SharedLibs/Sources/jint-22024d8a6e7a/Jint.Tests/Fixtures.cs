using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Jint.Expressions;
using Jint.Delegates;
using System.IO;
using Jint.Native;
using System.Reflection;
using Jint.Debugger;
using System.Security.Permissions;
using System.Diagnostics;
using System.Text;

namespace Jint.Tests {
    /// <summary>
    /// Summary description for UnitTest1
    /// </summary>
    [TestClass]
    public class Fixtures {
        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext { get; set; }

        protected object Test(Options options, string script) {
            return Test(options, script, jint => { });
        }

        protected object Test(string script) {
            return Test(Options.Ecmascript5 | Options.Strict, script);
        }

        protected object Test(string script, Action<JintEngine> action) {
            return Test(Options.Ecmascript5 | Options.Strict, script, action);
        }

        protected object Test(Options options, string script, Action<JintEngine> action)
        {
            var jint = new JintEngine(options)
                .AllowClr()
                .SetFunction("assert", new Action<object, object>(Assert.AreEqual))
                .SetFunction("fail", new Action<string>(Assert.Fail))
                .SetFunction("istrue", new Action<bool>(Assert.IsTrue))
                .SetFunction("isfalse", new Action<bool>(Assert.IsFalse))
                .SetFunction("print", new Action<string>(Console.WriteLine))
                .SetFunction("alert", new Action<string>(Console.WriteLine))
                .SetFunction("loadAssembly", new Action<string>(assemblyName => Assembly.Load(assemblyName)))
                .DisableSecurity();

            action(jint);

            var sw = new Stopwatch();
            sw.Start();

            var result = jint.Run(script);

            Console.WriteLine(sw.Elapsed);

            return result;
        }

        private void ExecuteEmbededScript(params string[] scripts) {
            const string prefix = "Jint.Tests.Scripts.";

            var assembly = Assembly.GetExecutingAssembly();
            var sb = new StringBuilder();
            foreach(var script in scripts) {
                var scriptPath = prefix + script;
                using (var sr = new StreamReader(assembly.GetManifestResourceStream(scriptPath)))
                {
                    sb.AppendLine(sr.ReadToEnd());        
                }
            }
            
            Test(sb.ToString());
        }

        [TestMethod]
        public void ShouldHandleDictionaryObjects() {
            var dic = new JsObject();
            dic["prop1"] = new JsNumber(1, JsNull.Instance);
            Assert.IsTrue(dic.HasProperty(new JsString("prop1", JsNull.Instance)));
            Assert.IsTrue(dic.HasProperty("prop1"));
            Assert.AreEqual(1, dic["prop1"].ToNumber());
        }

        [TestMethod]
        public void ShouldRunInRun() {
            var filename = Path.GetTempFileName();
            File.WriteAllText(filename, "a='bar'");

            var engine = new JintEngine().AddPermission(new FileIOPermission(PermissionState.Unrestricted));
            engine.AllowClr();
            engine.SetFunction("load", new Action<string>(delegate(string fileName) { using (var reader = File.OpenText(fileName)) { engine.Run(reader); } }));
            engine.SetFunction("print", new Action<string>(Console.WriteLine));
            engine.Run("var a='foo'; load('" + JintEngine.EscapteStringLiteral(filename) + "'); print(a);");

            File.Delete(filename);
        }

        [TestMethod]
        [ExpectedException(typeof(System.Security.SecurityException))]
        public void ShouldNotRunInRun() {
            var filename = Path.GetTempFileName();
            File.WriteAllText(filename, "a='bar'");

            var engine = new JintEngine().AddPermission(new FileIOPermission(PermissionState.None));
            engine.AllowClr();
            engine.SetFunction("load", new Action<string>(delegate(string fileName) { using (var reader = File.OpenText(fileName)) { engine.Run(reader); } }));
            engine.SetFunction("print", new Action<string>(Console.WriteLine));
            engine.Run("var a='foo'; load('" + JintEngine.EscapteStringLiteral(filename) + "'); print(a);");
        }

        [TestMethod]
        public void ShouldSupportCasting() {
            const string script = @";
                var value = Number(3);
                assert('number', typeof value);
                value = String(value); // casting
                assert('string', typeof value);
                assert('3', value);
            ";

            Test(script);

        }

        [TestMethod]
        public void ShouldCompareNullValues() {
            const string script = @";
                if(null == 1) 
                    assert(true, false); 

                if(null != null) 
                    assert(true, false); 
                
                if(null)
                    assert(true, false); 
        
                assert(true, true);
            ";

            Test(script);
        }


        [TestMethod]
        public void ShouldModifyIteratedCollection() {
            const string script = @";
                var values = [ 0, 1, 2 ];

                for (var v in values)
                {
                    values[v] = v * v;
                }

                assert(0, values[0]);
                assert(1, values[1]);
                assert(4, values[2]);
            ";

            Test(script);
        }

        [TestMethod]
        public void ShouldHandleTheMostSimple() {
            Test("var i = 1; assert(1, i);");
        }

        [TestMethod]
        public void ShouldHandleAnonymousFunctions() {
            const string script = @"
                function oksa(x, y) { return x + y; }
                assert(3, oksa(1, 2));
            ";

            Test(script);
        }

        [TestMethod]
        public void ShouldSupportUtf8VariableNames() {
            const string script = @"
                var 経済協力開発機構 = 'a strange variable';
                var Sébastien = 'a strange variable';
                assert('a strange variable', 経済協力開発機構);
                assert('a strange variable', Sébastien);
                assert('undefined', typeof sébastien);
            ";

            Test(script);
        }

        [TestMethod]
        public void ShouldHandleReturnAsSeparator() {
            Test(@" var i = 1; assert(1, i) ");
        }

        [TestMethod]
        public void ShouldHandleAssignment() {
            Test("var i; i = 1; assert(1, i);");
            Test("var i = 1; i = i + 1; assert(2, i);");
        }

        [TestMethod]
        public void ShouldHandleEmptyStatement() {
            Assert.AreEqual(1d, new JintEngine().Run(";;;;var i = 1;;;;;;;; return i;;;;;"));
        }

        [TestMethod]
        public void ShouldHandleFor() {
            Assert.AreEqual(9d, new JintEngine().Run("var j = 0; for(var i = 1; i < 10; i = i + 1) { j = j + 1; } return j;"));
        }

        [TestMethod]
        public void ShouldHandleSwitch() {
            Assert.AreEqual(1d, new JintEngine().Run("var j = 0; switch(j) { case 0 : j = 1; break; case 1 : j = 0; break; } return j;"));
            Assert.AreEqual(2d, new JintEngine().Run("var j = -1; switch(j) { case 0 : j = 1; break; case 1 : j = 0; break; default : j = 2; } return j;"));
        }

        [TestMethod]
        public void SwitchShouldFallBackWhenNoBreak() {
            Test(@"
                function doSwitch(input) {
                    var result = 0;
                    switch(input) {
                         case 'a':
                         case 'b':
                             result = 2;
                             break;
                          case 'c':
                              result = 3;
                             break;
                          case 'd':
                               result = 4;
                               break;
                          default:
                               break;
                    }
                    return result;
                }

                assert(2, doSwitch('a'));
                assert(0, doSwitch('z'));
                assert(2, doSwitch('b'));
                assert(3, doSwitch('c'));
                assert(4, doSwitch('d'));
            ");
        }

        [TestMethod]
        public void ShouldHandleVariableDeclaration() {
            Assert.AreEqual(null, new JintEngine().Run("var i; return i;"));
            Assert.AreEqual(1d, new JintEngine().Run("var i = 1; return i;"));
            Assert.AreEqual(2d, new JintEngine().Run("var i = 1 + 1; return i;"));
            Assert.AreEqual(3d, new JintEngine().Run("var i = 1 + 1; var j = i + 1; return j;"));
        }

        [TestMethod]
        public void ShouldHandleUndeclaredVariable() {
            Assert.AreEqual(1d, new JintEngine(Options.Ecmascript5).Run("i = 1; return i;"));
            Assert.AreEqual(2d, new JintEngine(Options.Ecmascript5).Run("i = 1 + 1; return i;"));
            Assert.AreEqual(3d, new JintEngine(Options.Ecmascript5).Run("i = 1 + 1; j = i + 1; return j;"));
        }

        [TestMethod]
        public void ShouldHandleStrings() {
            Assert.AreEqual("hello", new JintEngine().Run("return \"hello\";"));
            Assert.AreEqual("hello", new JintEngine().Run("return 'hello';"));

            Assert.AreEqual("hel'lo", new JintEngine().Run("return \"hel'lo\";"));
            Assert.AreEqual("hel\"lo", new JintEngine().Run("return 'hel\"lo';"));

            Assert.AreEqual("hel\"lo", new JintEngine().Run("return \"hel\\\"lo\";"));
            Assert.AreEqual("hel'lo", new JintEngine().Run("return 'hel\\'lo';"));

            Assert.AreEqual("hel\tlo", new JintEngine().Run("return 'hel\tlo';"));
            Assert.AreEqual("hel/lo", new JintEngine().Run("return 'hel/lo';"));
            Assert.AreEqual("hel//lo", new JintEngine().Run("return 'hel//lo';"));
            Assert.AreEqual("/*hello*/", new JintEngine().Run("return '/*hello*/';"));
            Assert.AreEqual("/hello/", new JintEngine().Run("return '/hello/';"));
        }

        [TestMethod]
        public void ShouldHandleExternalObject() {
            Assert.AreEqual(3d,
                new JintEngine()
                .SetParameter("i", 1)
                .SetParameter("j", 2)
                .Run("return i + j;"));
        }

        public bool ShouldBeCalledWithBoolean(TypeCode tc) {
            return tc == TypeCode.Boolean;
        }

        [TestMethod]
        public void ShouldHandleEnums() {
            Assert.AreEqual(TypeCode.Boolean,
                new JintEngine()
                .AllowClr()
                .Run("System.TypeCode.Boolean"));

            Assert.AreEqual(true,
                new JintEngine()
                .AllowClr()
                .SetParameter("clr", this)
                .Run("clr.ShouldBeCalledWithBoolean(System.TypeCode.Boolean)"));

        }

        [TestMethod]
        public void ShouldHandleNetObjects() {
            Assert.AreEqual("1",
                new JintEngine() // call Int32.ToString() 
                .SetParameter("i", 1)
                .Run("return i.ToString();"));
        }

        [TestMethod]
        public void ShouldReturnDelegateForFunctions() {
            const string script = "var ccat=function (arg1,arg2){ return arg1+' '+arg2; }";
            JintEngine engine = new JintEngine().SetFunction("print", new Action<string>(Console.WriteLine));
            engine.Run(script);
            Assert.AreEqual("Nicolas Penin", engine.CallFunction("ccat", "Nicolas", "Penin"));
        }

        [TestMethod]
        public void ShouldHandleFunctions() {
            const string square = @"function square(x) { return x * x; } return square(2);";
            const string fibonacci = @"function fibonacci(n) { if (n == 0) return 0; else return n + fibonacci(n - 1); } return fibonacci(10); ";

            Assert.AreEqual(4d, new JintEngine().Run(square));
            Assert.AreEqual(55d, new JintEngine().Run(fibonacci));
        }

        [TestMethod]
        public void ShouldCreateExternalTypes() {
            const string script = @"
                var sb = new System.Text.StringBuilder();
                sb.Append('hi, mom');
                sb.Append(3);	
                sb.Append(true);
                return sb.ToString();
                ";

            Assert.AreEqual("hi, mom3True", new JintEngine().AllowClr().Run(script));
        }

        [TestMethod]
        [ExpectedException(typeof(JintException))]
        public void ShouldNotAccessClr() {
            const string script = @"
                var sb = new System.Text.StringBuilder();
                sb.Append('hi, mom');
                sb.Append(3);	
                sb.Append(true);
                return sb.ToString();
                ";
            var engine = new JintEngine();
            Assert.AreEqual("hi, mom3True", engine.Run(script));
        }

        [ExpectedException(typeof(System.Security.SecurityException))]
        public void SecurityExceptionsShouldNotBeCaught() {
            const string script = @"
                try {
                    var sb = new System.Text.StringBuilder();
                    fail('should not have reached this code');
                } 
                catch (e) {
                    fail('should not have reached this code');
                }                
            ";
            var engine = new JintEngine();
            engine.Run(script);
        }

        [TestMethod]
        public void ShouldHandleStaticMethods() {
            const string script = @"
                var a = System.Int32.Parse('1');
                assert(1, ToDouble(a));
            ";

            Test(script);
        }

        [TestMethod]
        public void ShouldParseMultilineStrings() {
            const string script = @"
                assert('foobar', 'foo\
\
bar');            
";

            Test(script);
        }

        [TestMethod]
        public void ShouldEvaluateConsecutiveIfStatements() {
            const string script = @"
                var a = 0;
                
                if(a > 0)
                    a = -1;
                else
                    a = 0;

                if(a > 1)
                    a = -1;
                else
                    a = 1;

                if(a > 2)
                    a = -1;
                else
                    a = 2;

                assert(2, a);
            ";

            Test(script);
        }

        private static JsString GiveMeJavascript(JsNumber number, JsInstance instance) {
            return new JsString(number + instance.ToString(), JsNull.Instance);
        }

        [TestMethod]
        public void ShouldNotWrapJsInstancesIfExpected() {
            var engine = new JintEngine()
            .SetFunction("evaluate", new Func<JsNumber, JsInstance, JsString>(GiveMeJavascript));

            const string script = @"
                var r = evaluate(3, [1,2]);
                return r;
            ";

            var r = engine.Run(script, false);

            Assert.IsTrue(r is JsString);
            Assert.AreEqual("31,2", r.ToString());
        }

        [TestMethod]
        public void ShouldAssignBooleanValue() {
            const string script = @"
                function check(x) {
                    assert(false, x);    
                }

                var a = false;
                check(a);                
            ";

            Test(script);
        }

        [TestMethod]
        public void ShouldEvaluateFunctionDeclarationsFirst() {
            const string script = @"
                var a = false;
                assert(false, a);
                test();
                assert(true, a);
                
                function test() {
                    a = true;
                }
            ";

            Test(script);
        }

        [TestMethod]
        [ExpectedException(typeof(System.Security.SecurityException))]
        public void ShouldRunInLowTrustMode() {
            const string script = @"
                var a = System.Convert.ToInt32(1);
                var b = System.IO.Directory.GetFiles('c:');
            ";

            new JintEngine()
                .AllowClr()
                .Run(script);
        }

        [TestMethod]
        public void ShouldAllowSecuritySandBox() {
            var userDirectory = Path.GetTempPath();

            const string script = @"
                var b = System.IO.Directory.GetFiles(userDir);
            ";

            new JintEngine()
                .AllowClr()
                .SetParameter("userDir", userDirectory)
                .AddPermission(new FileIOPermission(FileIOPermissionAccess.PathDiscovery, userDirectory))
                .Run(script);
        }


        [TestMethod]
        public void ShouldSetClrProperties() {
            // Ensure assembly is loaded
            var a = typeof(System.Windows.Forms.Form);
            var b = a.Assembly; // Force loading in Release mode, otherwise code is optimized
            const string script = @"
                var frm = new System.Windows.Forms.Form();
                frm.Text = 'Test';
                return frm.Text; 
            ";

            var result = new JintEngine()
                .AddPermission(new UIPermission(PermissionState.Unrestricted))
                .AllowClr()
                .Run(script);

            Assert.AreEqual("Test", result.ToString());
        }

        [TestMethod]
        public void ShouldHandleCustomMethods() {
            Assert.AreEqual(9d, new JintEngine()
                .SetFunction("square", new Func<double, double>(a => a * a))
                .Run("return square(3);"));

            new JintEngine()
                .SetFunction("print", new Action<string>(Console.Write))
                .Run("print('hello');");

            const string script = @"
                function square(x) { 
                    return multiply(x, x); 
                }; 

                return square(4);
            ";

            var result =
                new JintEngine()
                .SetFunction("multiply", new Func<double, double, double>((x, y) => x * y))
                .Run(script);

            Assert.AreEqual(16d, result);
        }

        [TestMethod]
        public void ShouldHandleDirectNewInvocation() {
            Assert.AreEqual("c", new JintEngine().AllowClr()
                .Run("return new System.Text.StringBuilder('c').ToString();"));
        }

        [TestMethod]
        public void ShouldHandleGlobalVariables() {
            const string program = @"
                var i = 3;
                function calculate() {
                    return i*i;
                }
                return calculate();
            ";

            Assert.AreEqual(9d, new JintEngine()
                .Run(program));
        }

        [TestMethod]
        public void ShouldHandleObjectClass() {
            const string program = @"
                var userObject = new Object();
                userObject.lastLoginTime = new Date();
                return userObject.lastLoginTime;
            ";

            object result = new JintEngine().Run(program);

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(DateTime));
        }

        [TestMethod]
        public void ShouldHandleIndexedProperties() {
            const string program = @"
                var userObject = { };
                userObject['lastLoginTime'] = new Date();
                return userObject.lastLoginTime;
            ";

            object result = new JintEngine().Run(program);

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(DateTime));
        }

        [TestMethod]
        public void ShouldAssignProperties() {
            const string script = @"
                function sayHi(x) {
                    alert('Hi, ' + x + '!');
                }

                sayHi.text = 'Hello World!';
                sayHi['text2'] = 'Hello World... again.';

                assert('Hello World!', sayHi['text']); 
                assert('Hello World... again.', sayHi.text2); 
                ";

            Test(script);
        }

        [TestMethod]
        public void ShouldStoreFunctionsInArray() {
            const string script = @"

                // functions stored as array elements
                var arr = [];
                arr[0] = function(x) { return x * x; };
                arr[1] = arr[0](2);
                arr[2] = arr[0](arr[1]);
                arr[3] = arr[0](arr[2]);
                
                // displays 256
                assert(256, arr[3]);
            ";

            Test(script);
        }

        [TestMethod]
        public void ShouldNotConflictWithClrMethods() {
            const string script = @"
                assert(true, System.Math.Max(1, 2) == 2);
                assert(true, System.Math.Min(1, 2) == 1);
            ";

            Test(script);
        }

        [TestMethod]
        public void ShouldCreateObjectLiterals() {
            const string script = @"
                var myDog = {
                    'name' : 'Spot',
                    'bark' : function() { return 'Woof!'; },
                    'displayFullName' : function() {
                        return this.name + ' The Alpha Dog';
                    },
                    'chaseMrPostman' : function() { 
                        // implementation beyond the scope of this article 
                    }    
                };
                assert('Spot The Alpha Dog', myDog.displayFullName()); 
                assert('Woof!', myDog.bark()); // Woof!
            ";

            Test(script);
        }

        [TestMethod]
        public void ShouldHandleFunctionsAsObjects() {
            const string script = @"
                // assign an anonymous function to a variable
                var greet = function(x) {
                    return 'Hello, ' + x;
                };

                assert('Hello, MSDN readers', greet('MSDN readers'));

                // passing a function as an argument to another
                function square(x) {
                    return x * x;
                }
                function operateOn(num, func) {
                    return func(num);
                }
                // displays 256
                assert(256, operateOn(16, square));

                // functions as return values
                function makeIncrementer() {
                    return function(x) { return x + 1; };
                }
                var inc = makeIncrementer();
                // displays 8
                assert(8, inc(7));
                ";

            Test(script);

            Test(@"var Test = {};
Test.FakeButton = function() { };
Test.FakeButton.prototype = {};
var fakeButton = new Test.FakeButton();");
        }

        [TestMethod]
        public void ShouldOverrideDefaultFunction() {
            const string script = @"

                // functions as object properties
                var obj = { 'toString' : function() { return 'This is an object.'; } };
                // calls obj.toString()
                assert('This is an object.', obj.toString());
            ";

            Test(script);
        }

        [TestMethod]
        public void ShouldHandleFunctionConstructor() {
            const string script = @"
                var func = new Function('x', 'return x * x;');
                var r = func(3);
                assert(9, r);
            ";

            Test(script);
        }

        [TestMethod]
        public void ShouldContinueAfterFunctionCall() {
            const string script = @"
                function fib(x) {
                    if (x==0) return 0;
                    if (x==1) return 1;
                    if (x==2) return 2;
                    return fib(x-1) + fib(x-2);
                }

                var x = fib(0);
                
                return 'beacon';
                ";

            Assert.AreEqual("beacon", Test(script).ToString());
        }

        [TestMethod]
        public void ShouldRetainGlobalsThroughRuns() {
            var jint = new JintEngine();

            jint.Run("var i = 3; function square(x) { return x*x; }");

            Assert.AreEqual(3d, jint.Run("return i;"));
            Assert.AreEqual(9d, jint.Run("return square(i);"));
        }

        [TestMethod]
        public void ShouldDebugScripts() {
            var jint = new JintEngine()
            .SetDebugMode(true);
            jint.BreakPoints.Add(new BreakPoint(4, 22)); // return x*x;

            jint.Step += (sender, info) => Assert.IsNotNull(info.CurrentStatement);

            bool brokeOnReturn = false;

            jint.Break += (sender, info) => {
                Assert.IsNotNull(info.CurrentStatement);
                Assert.IsTrue(info.CurrentStatement is ReturnStatement);
                Assert.AreEqual(3, Convert.ToInt32(info.Locals["x"].Value));

                brokeOnReturn = true;
            };

            jint.Run(@"
                var i = 3; 
                function square(x) { 
                    return x*x; 
                }

                return square(i);
            ");

            Assert.IsTrue(brokeOnReturn);

        }

        [TestMethod]
        public void ShouldBreakInLoops() {
            var jint = new JintEngine()
                .SetDebugMode(true);
            jint.BreakPoints.Add(new BreakPoint(4, 22)); // x += 1;

            jint.Step += (sender, info) => Assert.IsNotNull(info.CurrentStatement);

            bool brokeInLoop = false;

            jint.Break += (sender, info) => {
                Assert.IsNotNull(info.CurrentStatement);
                Assert.IsTrue(info.CurrentStatement is ExpressionStatement);
                Assert.AreEqual(7, Convert.ToInt32(info.Locals["x"].Value));

                brokeInLoop = true;
            };

            jint.Run(@"
                var x = 7;
                for(var i=0; i<3; i++) { 
                    x += i; 
                    return;
                }
            ");

            Assert.IsTrue(brokeInLoop);
        }

        [TestMethod]
        public void ShouldBreakOnCondition() {
            JintEngine jint = new JintEngine()
            .SetDebugMode(true);
            jint.BreakPoints.Add(new BreakPoint(4, 22, "x == 2;")); // return x*x;

            jint.Step += (sender, info) => Assert.IsNotNull(info.CurrentStatement);

            bool brokeOnReturn = false;

            jint.Break += (sender, info) => {
                Assert.IsNotNull(info.CurrentStatement);
                Assert.IsTrue(info.CurrentStatement is ReturnStatement);
                Assert.AreEqual(2, Convert.ToInt32(info.Locals["x"].Value));

                brokeOnReturn = true;
            };

            jint.Run(@"
                var i = 3; 
                function square(x) { 
                    return x*x; 
                }
                
                square(1);
                square(2);
                square(3);
            ");

            Assert.IsTrue(brokeOnReturn);
        }

        [TestMethod]
        public void ShouldHandleInlineCLRMethodCalls() {
            string script = @"
                var box = new Jint.Tests.Box();
                box.SetSize(ToInt32(100), ToInt32(100));
                assert(100, Number(box.Width));
                assert(100, Number(box.Height));
            ";
            Test(script);
        }

        [TestMethod]
        public void ShouldHandleStructs() {
            const string script = @"
                var size = new Jint.Tests.Size();
                size.Width = 10;
                assert(10, Number(size.Width));
                assert(0, Number(size.Height));
            ";
            Test(script);
        }

        [TestMethod]
        public void ShouldHandleFunctionScopes() {
            const string script = @"
                var success = false;
                var $ = {};

                (function () { 
                    
                    function a(x) {
                        success = x;                                   
                    }
                    
                    $.b = function () {
                        a(true);
                    }

                }());
                
                $.b();

                ";

            Test(script);
        }

        [TestMethod]
        public void ShouldHandleLoopScopes() {
            const string script = @"
                var f = function() { var i = 10; }
                for(var i=0; i<3; i++) { f(); }
                assert(3, i);

                f = function() { i = 10; }
                for(i=0; i<3; i++) { f(); }
                assert(11, i);

                f = function() { var i = 10; }
                for(i=0; i<3; i++) { f(); }
                assert(3, i);

                f = function() { i = 10; }
                for(var i=0; i<3; i++) { f(); }
                assert(11, i);
                ";

            Test(script);
        }

        [TestMethod]
        public void ShouldExecuteSingleScript() {
            var assembly = Assembly.GetExecutingAssembly();
            var program = new StreamReader(assembly.GetManifestResourceStream("Jint.Tests.Scripts.Date.js")).ReadToEnd();
            Test(program);
        }

        [TestMethod]
        public void ShouldCascadeEquals() {
            Test("var a, b; a=b=1; assert(1,a);assert(1,b);");
        }

        [TestMethod]
        public void ShouldParseScripts() {
            var assembly = Assembly.GetExecutingAssembly();
            foreach (var resx in assembly.GetManifestResourceNames()) {
                // Ignore scripts not in /Scripts
                if (!resx.Contains(".Parse")) {
                    continue;
                }

                var program = new StreamReader(assembly.GetManifestResourceStream(resx)).ReadToEnd();
                if (program.Trim() == String.Empty) {
                    continue;
                }
                Trace.WriteLine(Path.GetFileNameWithoutExtension(resx));
                JintEngine.Compile(program, true);
            }
        }

        [TestMethod]
        public void ShouldHandleNativeTypes() {

            var jint = new JintEngine()
            .SetDebugMode(true)
            .SetFunction("assert", new Action<object, object>(Assert.AreEqual))
            .SetFunction("print", new Action<string>(System.Console.WriteLine))
            .SetParameter("foo", "native string");

            jint.Run(@"
                assert(7, foo.indexOf('string'));            
            ");
        }

        [TestMethod]
        public void ClrNullShouldBeConverted() {

            var jint = new JintEngine()
            .SetDebugMode(true)
            .SetFunction("assert", new Action<object, object>(Assert.AreEqual))
            .SetFunction("print", new Action<string>(System.Console.WriteLine))
            .SetParameter("foo", null);

            // strict equlity ecma 262.3 11.9.6 x === y: If type of (x) is null return true.
            jint.Run(@"
                assert(true, foo == null);
                assert(true, foo === null);
            ");
        }

        public void RunMozillaTests(string folder) {
            var assembly = Assembly.GetExecutingAssembly();
            var shell = new StreamReader(assembly.GetManifestResourceStream("Jint.Tests.shell.js")).ReadToEnd();
            var extensions = new StreamReader(assembly.GetManifestResourceStream("Jint.Tests.extensions.js")).ReadToEnd();

            var resources = new List<string>();
            foreach (var resx in assembly.GetManifestResourceNames()) {
                // Ignore scripts not in /Scripts
                if (!resx.Contains(".ecma_3.") || !resx.Contains(folder)) {
                    continue;
                }

                resources.Add(resx);
            }

            resources.Sort();

            //Run the shell first if defined
            string additionalShell = null;
            if (resources[resources.Count - 1].EndsWith("shell.js")) {
                additionalShell = resources[resources.Count - 1];
                resources.RemoveAt(resources.Count - 1);
                additionalShell = new StreamReader(assembly.GetManifestResourceStream(additionalShell)).ReadToEnd();
            }

            foreach (var resx in resources) {
                var program = new StreamReader(assembly.GetManifestResourceStream(resx)).ReadToEnd();
                Console.WriteLine(Path.GetFileNameWithoutExtension(resx));

                StringBuilder output = new StringBuilder();
                StringWriter sw = new StringWriter(output);

                var jint = new JintEngine(Options.Ecmascript5) // These tests doesn't work with strict mode
                .SetDebugMode(true)
                .SetFunction("print", new Action<string>(sw.WriteLine));

                jint.Run(extensions);
                jint.Run(shell);
                jint.Run("test = _test;");
                if (additionalShell != null) {
                    jint.Run(additionalShell);
                }

                try {
                    jint.Run(program);
                    string result = sw.ToString();
                    if (result.Contains("FAILED")) {
                        Assert.Fail(result);
                    }
                }
                catch (Exception e) {
                    jint.Run("print('Error in : ' + gTestfile)");
                    Assert.Fail(e.Message);
                }
            }
        }

        [TestMethod]
        [Ignore]
        public void ShouldExecuteEcmascript5TestsScripts() {
            var assembly = Assembly.GetExecutingAssembly();
            var extensions = new StreamReader(assembly.GetManifestResourceStream("Jint.Tests.extensions.js")).ReadToEnd();

            var resources = new List<string>();
            foreach (var resx in assembly.GetManifestResourceNames()) {
                // Ignore scripts not in /Scripts
                if (!resx.Contains(".ecma_5.") || resx.Contains(".Scripts.")) {
                    continue;
                }

                resources.Add(resx);
            }

            resources.Sort();

            //Run the shell first if defined
            string additionalShell = null;
            if (resources[resources.Count - 1].EndsWith("shell.js")) {
                additionalShell = resources[resources.Count - 1];
                resources.RemoveAt(resources.Count - 1);
                additionalShell = new StreamReader(assembly.GetManifestResourceStream(additionalShell)).ReadToEnd();
            }

            foreach (var resx in resources) {
                var program = new StreamReader(assembly.GetManifestResourceStream(resx)).ReadToEnd();
                Console.WriteLine(Path.GetFileNameWithoutExtension(resx));

                var jint = new JintEngine()
                .SetDebugMode(true)
                .SetFunction("print", new Action<string>(System.Console.WriteLine));

                jint.Run(extensions);
                //jint.Run(shell);
                jint.Run("test = _test;");
                if (additionalShell != null) {
                    jint.Run(additionalShell);
                }

                try {
                    jint.Run(program);
                }
                catch (Exception e) {
                    jint.Run("print('Error in : ' + gTestfile)");
                    Console.WriteLine(e.Message);
                }
            }
        }

        public List<int> FindAll(List<int> source, Predicate<int> predicate) {
            var result = new List<int>();

            foreach (var i in source) {
                var obj = predicate(i);

                if (obj) {
                    result.Add(i);
                }
            }

            return result;
        }

        [TestMethod]
        public void ShouldHandleStrictMode() {
            //Strict mode enabled
            var engine = new JintEngine(Options.Strict)
            .SetFunction("assert", new Action<object, object>(Assert.AreEqual))
            ;
            engine.Run(@"
            try{
                var test1=function(eval){}
                //should not execute the next statement
                assert(true, false);
            }
            catch(e){
                assert(true, true);
            }
            try{
                (function() {
                    function test2(eval){}
                    //should not execute the next statement
                    assert(true, false);
                })();
            }
            catch(e){
                assert(true, true);
            }");

            //Strict mode disabled
            engine = new JintEngine(Options.Ecmascript3)
            .SetFunction("assert", new Action<object, object>(Assert.AreEqual))
            ;
            engine.Run(@"
            try{
                var test1=function(eval){}
                assert(true, true);
            }
            catch(e){
                assert(true, false);
            }
            try{
                (function() {
                    function test2(eval){}
                    assert(true, true);
                })();
            }
            catch(e){
                assert(true, false);
            }");
        }

        [TestMethod]
        public void ShouldHandleMultipleRunsInSameScope() {
            var jint = new JintEngine()
                .SetFunction("assert", new Action<object, object>(Assert.AreEqual))
                .SetFunction("print", new Action<string>(System.Console.WriteLine));

            jint.Run(@" var g = []; function foo() { assert(0, g.length); }");
            jint.Run(@" foo();");
        }

        [TestMethod]
        public void ShouldHandleClrArrays() {
            var values = new int[] { 2, 3, 4, 5, 6, 7 };
            var jint = new JintEngine()
            .SetDebugMode(true)
            .SetParameter("a", values)
            .AllowClr();

            Assert.AreEqual(3, jint.Run("a[1];"));
            jint.Run("a[1] = 4");
            Assert.AreEqual(4, jint.Run("a[1];"));
            Assert.AreEqual(4, values[1]);

        }

        [TestMethod]
        public void ShouldHandleClrDictionaries() {
            var dic = new Dictionary<string, int> { { "a", 1 }, { "b", 2 }, { "c", 3 } };

            var jint = new JintEngine()
            .AllowClr()
            .SetDebugMode(true)
            .SetParameter("dic", dic);

            Assert.AreEqual(1, jint.Run("return dic['a'];"));
            jint.Run("dic['a'] = 4");
            Assert.AreEqual(4, jint.Run("return dic['a'];"));
            Assert.AreEqual(4, dic["a"]);
        }

        [TestMethod]
        public void ShouldEvaluateIndexersAsClrProperties() {
            var box = new Box { Width = 10, Height = 20 };

            var jint = new JintEngine()
            .AllowClr()
            .SetDebugMode(true)
            .SetParameter("box", box);

            Assert.AreEqual(10, jint.Run("return box.Width"));
            Assert.AreEqual(10, jint.Run("return box['Width']"));
            jint.Run("box['Height'] = 30;");

            Assert.AreEqual(30, box.Height);

            jint.Run("box.Height = 18;");
            Assert.AreEqual(18, box.Height);
        }

        [TestMethod]
        public void ShouldEvaluateIndexersAsClrFields() {
            var box = new Box { width = 10, height = 20 };

            var jint = new JintEngine()
            .SetDebugMode(true)
            .AllowClr()
            .SetParameter("box", box);

            Assert.AreEqual(10, jint.Run("return box.width"));
            Assert.AreEqual(10, jint.Run("return box['width']"));
            jint.Run("box['height'] = 30;");

            Assert.AreEqual(30, box.height);

            jint.Run("box.height = 18;");

            Assert.AreEqual(18, box.height);

        }

        [TestMethod]
        public void ShouldFindOverloadWithNullParam() {
            var box = new Box { Width = 10, Height = 20 };

            var jint = new Jint.JintEngine()
            .SetDebugMode(true)
            .SetFunction("assert", new Action<object, object>(Assert.AreEqual))
            .SetParameter("box", box);

            jint.Run(@"
                assert(1, Number(box.Foo(1)));
                assert(2, Number(box.Foo(2, null)));    
            ");
        }

        [TestMethod]
        public void ShouldHandlePropertiesOnFunctions() {
            Test(@"
                HelloWorld.webCallable = 'GET';
                function HelloWorld()
                {
                    return 'Hello from Javascript!';
                }
                
                assert('GET', HelloWorld.webCallable);
            ");

        }

        [TestMethod]
        public void ShouldCatchNotDefinedVariable() {
            Test(@"
                try {
                    a = b;
                    assert(true, false);
                } 
                catch(e) {
                }

                assert('undefined', typeof foo);
                
                try {
                    var y;
                    assert(false, y instanceof Foo);
                    assert(true, false);
                } 
                catch(e) {
                }                
            ");
        }

        [TestMethod]
        public void ShouldNotThrowOverflowExpcetion() {
            var jint = new JintEngine();
            jint.SetParameter("box", new Box());
            jint.Run("box.Write(new Date);");

        }

        [TestMethod]
        public void ShouldNotReproduceBug85418() {
            var engine = new JintEngine();
            engine.SetParameter("a", 4);
            Assert.AreEqual(4, engine.Run("a"));
            Assert.AreEqual(4d, engine.Run("4"));
            Assert.AreEqual(true, engine.Run("a == 4"));
            Assert.AreEqual(true, engine.Run("4 == 4"));
            Assert.AreEqual(true, engine.Run("a == a"));
        }

        [TestMethod]
        public void ShouldShortCircuitBooleanOperators() {
            Test(@"
                var called = false;
                function dontcallme() {
                    called = true;
                }
                
                assert(true, true || dontcallme());
                assert(false, called);

                assert(false, false && dontcallme());
                assert(false, called);

                ");
        }

        [TestMethod]
        public void UndefinedEqualsToNullShouldBeTrue() {
            Test(@"
                assert(true, undefined == null);
                assert(false, undefined === null);
                ");
        }

        [TestMethod]
        public void NumbersShouldEqualTheirStrings() {
            Test(@"
                assert(true, 5 == '5');
                assert(true, 5.1 == '5.1');
                assert(false, 5 === '5');
                ");
        }

        [TestMethod]
        public void AccessorsScriptShouldPassTests() {
            ExecuteEmbededScript("Accessors.js");
        }

        [TestMethod]
        public void ArgumentsScriptShouldPassTests() {
            ExecuteEmbededScript("Arguments.js");
        }

        [TestMethod]
        public void ArraysScriptShouldPassTests() {
            ExecuteEmbededScript("Arrays.js");
        }

        [TestMethod]
        public void BlocksScriptShouldPassTests() {
            ExecuteEmbededScript("Blocks.js");
        }

        [TestMethod]
        public void BooleanScriptShouldPassTests() {
            ExecuteEmbededScript("Boolean.js");
        }

        [TestMethod]
        public void ChainConstructorsScriptShouldPassTests() {
            ExecuteEmbededScript("ChainConstructors.js");
        }

        [TestMethod]
        public void ClosuresScriptShouldPassTests() {
            ExecuteEmbededScript("Closures.js");
        }

        [TestMethod]
        public void ClrScriptShouldPassTests() {
            ExecuteEmbededScript("Clr.js");
        }

        [TestMethod]
        public void CoffeeScriptShouldPassTests()
        {
            ExecuteEmbededScript("coffeescript.js", "coffeescript-suite.js");
            ExecuteEmbededScript("coffeescript-min.js", "coffeescript-suite.js");
        }

        [TestMethod]
        public void CommentsScriptShouldPassTests() {
            ExecuteEmbededScript("Comments.js");
        }

        [TestMethod]
        public void DateScriptShouldPassTests() {
            ExecuteEmbededScript("Date.js");
        }

        [TestMethod]
        public void FunctionScriptShouldPassTests() {
            ExecuteEmbededScript("Function.js");
        }

        [TestMethod]
        public void FunctionAsConstrutorScriptShouldPassTests() {
            ExecuteEmbededScript("FunctionAsConstructor.js");
        }

        [TestMethod]
        public void GlobalScriptShouldPassTests()
        {
            ExecuteEmbededScript("Global.js");
        }

        [TestMethod]
        public void HoistingScriptShouldPassTests() {
            ExecuteEmbededScript("Hoisting.js");
        }

        [TestMethod]
        public void InOperatorScriptShouldPassTests() {
            ExecuteEmbededScript("InOperator.js");
        }

        [TestMethod]
        public void JsonScriptShouldPassTests()
        {
            ExecuteEmbededScript("Json.js");
        }

        [TestMethod]
        public void Json2ScriptShouldPassTests()
        {
            ExecuteEmbededScript("json2.js");
        }

        [TestMethod]
        public void LoopsScriptShouldPassTests() {
            ExecuteEmbededScript("Loops.js");
        }

        [TestMethod]
        public void MathScriptShouldPassTests() {
            ExecuteEmbededScript("Math.js");
        }

        [TestMethod]
        public void NumberScriptShouldPassTests() {
            ExecuteEmbededScript("Number.js");
        }

        [TestMethod]
        public void ObjectScriptShouldPassTests() {
            ExecuteEmbededScript("Object.js");
        }

        [TestMethod]
        public void OperatorsScriptShouldPassTests() {
            ExecuteEmbededScript("Operators.js");
        }

        [TestMethod]
        public void PrecedenceScriptShouldPassTests()
        {
            ExecuteEmbededScript("Precedence.js");
        }

        [TestMethod]
        public void PrivateMembersScriptShouldPassTests()
        {
            ExecuteEmbededScript("PrivateMembers.js");
        }

        [TestMethod]
        public void PrototypeInheritanceScriptShouldPassTests() {
            ExecuteEmbededScript("PrototypeInheritance.js");
        }

        [TestMethod]
        public void RegExpScriptShouldPassTests() {
            ExecuteEmbededScript("RegExp.js");
        }

        [TestMethod]
        public void SimpleClassScriptShouldPassTests() {
            ExecuteEmbededScript("SimpleClass.js");
        }

        [TestMethod]
        public void StaticMethodsScriptShouldPassTests() {
            ExecuteEmbededScript("StaticMethods.js");
        }

        [TestMethod]
        public void StringScriptShouldPassTests() {
            ExecuteEmbededScript("String.js");
        }

        [TestMethod]
        public void TernaryScriptShouldPassTests() {
            ExecuteEmbededScript("Ternary.js");
        }

        [TestMethod]
        public void ThisInDifferentScopesScriptShouldPassTests() {
            ExecuteEmbededScript("ThisInDifferentScopes.js");
        }

        [TestMethod]
        public void TryCatchScriptShouldPassTests() {
            ExecuteEmbededScript("TryCatch.js");
        }

        [TestMethod]
        public void TypeofScriptShouldPassTests() {
            ExecuteEmbededScript("typeof.js");
        }

        [TestMethod]
        public void UnderscoreScriptShouldPassTests()
        {
            ExecuteEmbededScript("underscore.js", "underscore-suite.js");
            //ExecuteEmbededScript("underscore-min.js", "underscore-suite.js");
        }

        [TestMethod]
        public void WithScriptShouldPassTests() {
            ExecuteEmbededScript("With.js");
        }

        [TestMethod]
        public void InstanceOfScriptShouldPassTests() {
            ExecuteEmbededScript("instanceOf.js");
        }

        [TestMethod]
        public void FlowScriptShouldPassTests() {
            ExecuteEmbededScript("Flow.js");
        }

        [TestMethod]
        public void RandomValuesShouldNotRepeat() {
            Test(@"
                for(var i=0; i<100; i++){
                    assert(false, Math.random() == Math.random());
                }
            ");
        }

        [TestMethod]
        public void MaxRecursionsShouldBeDetected() {
            Test(@"
                function doSomething(){
                    doSomethingElse();
                }

                function doSomethingElse(){
                    doSomething();
                }

                try {
                    doSomething();
                    assert(true, false);
                }
                catch (e){
                    return;                
                }
                ");
        }

        [TestMethod]
        public void ObjectShouldBePassedToDelegates() {
            var engine = new JintEngine();
            engine.SetFunction("render", new Action<object>(s => Console.WriteLine(s)));

            const string script =
                @"
                var contact = {
                    'Name': 'John Doe',
                    'PhoneNumbers': [ 
                    {
                       'Location': 'Home',
                       'Number': '555-555-1234'
                    },
                    {
                        'Location': 'Work',
                        'Number': '555-555-9999 Ext. 123'
                    }
                    ]
                };

                render(contact.Name);
                render(contact.toString());
                render(contact);
            ";

            engine.Run(script);
        }

        [TestMethod]
        public void IndexerShouldBeEvaluatedBeforeUsed() {
            Test(@"
                var cat = {
                    name : 'mega cat',
                    prop: 'name',
                    hates: 'dog'
                };

                var prop = 'hates';
                assert('dog', cat[prop]);

                ");
        }

        [TestMethod]
        public void ShouldParseCoffeeScript() {
            Test(@"
                var xhr = new (String || Number)('123');
                var type = String || Number;
                var x = new type('123');
                assert('123', x);
            ");
        }

        [TestMethod]
        public void ShouldReturnUndefined()
        {
            Test(@"
                function a() {  };
                assert(undefined, a());
            ");
        }

        [TestMethod]
        public void StaticMemberAfterUndefinedReference() {
            var engine = new Jint.JintEngine().AllowClr();

            Assert.AreEqual(System.String.Format("{0}", 1), engine.Run("System.String.Format('{0}', 1)"));
            Assert.AreEqual("undefined", engine.Run("typeof thisIsNotDefined"));
            Assert.AreEqual(System.String.Format("{0}", 1), engine.Run("System.String.Format('{0}', 1)"));
        }

        [TestMethod]
        public void MozillaNumber() {
            RunMozillaTests("Number");
        }

        [TestMethod]
        public void ShouldDetectErrors()
        {
            string errors;
            Assert.IsTrue(JintEngine.HasErrors("var s = @string?;", out errors));
            Assert.IsTrue(JintEngine.HasErrors(")(----", out errors));
        }

        [TestMethod, Ignore] public void ShouldNotDetectErrors()
        {
            // todo: fix
            string errors;
            Assert.IsFalse(JintEngine.HasErrors("var s = 'bar'", out errors));
            Assert.IsFalse(JintEngine.HasErrors("", out errors));
            Assert.IsFalse(JintEngine.HasErrors("// comment", out errors));
        }

        [TestMethod]
        public void ShouldHandleBadEnums()
        {
            Test(@"
                assert('Name', Jint.Tests.FooEnum.Name.toString());
                assert('GetType', Jint.Tests.FooEnum.GetType.toString());
                assert('IsEnum', Jint.Tests.FooEnum.IsEnum.toString());
                assert('System', Jint.Tests.FooEnum.System.toString());

                // still can access hidden Type properties
                assert('FooEnum',Jint.Tests.FooEnum.get_Name());
            ");
        }

        [TestMethod]
        [ExpectedException(typeof(JintException))]
        public void RunningInvalidScriptSourceShouldThrow() {
            new JintEngine().Run("var s = @string?;");
        }

        [TestMethod]
        public void UseOfUndefinedVariableShouldThrowAnException() {
            Test(@"
                try {
                    if(abc) {
                    }
                    fail('should have thrown an Error');
                }
                catch (e) {
                    return;
                }
                fail('should have caught an Error');

                try {
                    do{
                    } while(abc);

                fail('should have thrown an Error');
                }
                catch (e) {
                    return;
                }
                fail('should have caught an Error');

            ");
        }

        [TestMethod]
        public void CallingANonMethodShouldThrowAnException() {
            Test(@"
                try {
                    var x = { prop: 'abc'};
                    x.prop();
                    fail('should have thrown an Error');
                }
                catch (e) {
                    return;
                }
                fail('should have caught an Error');
            ");
        }

        [TestMethod]
        public void FunctionsShouldBeDeclaredInTheirScope()
        {
            Test(@"
                function foo() {
                    function bar() {
                    }
                    
                    bar();
                }
                
                var bar = 1;
                foo();
                assert(1, bar);                
            ");
        }

        [TestMethod]
        public void ScopesShouldNotExpand() {
            Test(@"
                function foo() {
                    var i;
                    for(i=2;i<3;i++);
                }
                
                function bar() {
                    var i=1;
                    foo();

                    assert(1, i);
                }
                
                bar();
            ");
        }

        [TestMethod]
        public void ShouldHandleCommaSeparatedDeclarations() {
            Test(@"
                var i, j=1, k=3*2;

                function foo() {
                    var l, m=1, n=3*2;

                    assert(undefined, l);
                    assert(1, m);
                    assert(6, n);
                }

                assert(undefined, i);
                assert(1, j);
                assert(6, k);

                foo();
            ");
        }

        [TestMethod]
        public void ClrExceptionsShouldNotBeLost() {
            try {
                Test(@"foo();",
                     jint => jint.SetFunction("foo", new Action(delegate { throw new ArgumentNullException("bar"); })));
                Assert.Fail();
            }
            catch(JintException e) {
                var ane = e.InnerException as ArgumentNullException;
                Assert.IsNotNull(e);
                Assert.AreEqual("bar", ane.ParamName);
                return;
            }
        }

        [TestMethod]
        public void DelegateShouldBeAbleToUseCallFunction()
        {
            Test(@"
                    var square = function(x) { return x*x;}
                    assert(9, callme(3));
                ",
                 jint => jint.SetFunction("callme", new Func<double, object>(x => jint.CallFunction("square", x)))
            );
            Test(
                @"
                    assert(true,callme(function() { return true; } ));
                ",
                jint => jint.SetFunction("callme", new Func<Func<bool>, object>(
                    callback => {
                        return callback();
                    }
                ))
            );
        }

        [TestMethod]
        public void NumberMethodsShouldWorkOnMarshalledNumbers() {
            new JintEngine()
                .DisableSecurity()
                .SetFunction("getDouble", new Func<double>(() => { return 11.34543; }))
                .SetFunction("getInt", new Func<int>(() => { return 13; }))
                .SetFunction("print", new Action<string>(s => Console.WriteLine(s)))
                .Run(@"
                    print( getDouble().toFixed(2) );
                    print( getInt().toFixed(2) );
                ");
        }

        [TestMethod]
        public void ShouldNotReturnDateInUniversalTime() {
            var date = (DateTime)new JintEngine()
                .Run(@"
                    return new Date(2012, 0, 1);
                ");

            Assert.AreEqual(2012, date.Year);
            Assert.AreEqual(1, date.Month);
            Assert.AreEqual(1, date.Day);
            Assert.AreEqual(0, date.Hour);
            Assert.AreEqual(0, date.Minute);
            Assert.AreEqual(0, date.Second);
        }

        [TestMethod]
        public void ShouldNotReferenceThisAsGlobalScopeInDetachedFunctionInStrictMode() {
            new JintEngine(Options.Ecmascript5)
                .SetFunction("assert", new Action<object, object>(Assert.AreEqual))
                .Run(@"
                    var x = 1;
                    var module = {
                        x: 2,
                        getx: function() {
                            return this.x;
                        }
                    }
                    assert(2, module.getx());

                    var getx = module.getx;
                    assert(1, getx());
                ");

            var result = new JintEngine(Options.Ecmascript5 | Options.Strict)
                .SetFunction("assert", new Action<object, object>(Assert.AreEqual))
                .Run(@"
                    var x = 1;
                    var module = {
                        x: 2,
                        getx: function() {
                            try {
                                return this.x;
                            }
                            catch(e) {
                                return null;
                            }
                        }
                    }
                    assert(2, module.getx());

                    var global_getx = module.getx;
                    assert(null, global_getx());

                    assert(1, global_getx.call(this));
                ");
        }

        [TestMethod]
        public void ShouldThrowErrorWhenAssigningUndeclaredVariableInStrictMode() {
            var engine = new JintEngine(Options.Ecmascript5 | Options.Strict)
                .SetFunction("assert", new Action<object, object>(Assert.AreEqual));
            var x = engine.Run(@"
                try {
                    x = 1;
                    return x;
                } catch(e) {
                    return 'error';
                }
            ");

            Assert.AreEqual("error", x);
        }
    }

    public struct Size {
        public int Width;
        public int Height;
    }

    public enum FooEnum
    {
        Name = 1,
        GetType = 2,
        IsEnum = 3,
        System = 4
    }

    public class Box {
        // public fields
        public int width;
        public int height;

        // public properties
        public int Width { get; set; }
        public int Height { get; set; }

        public void SetSize(int width, int height) {
            Width = width;
            Height = height;
        }

        public int Foo(int a, object b) {
            return a;
        }

        public int Foo(int a) {
            return a;
        }

        public void Write(object value) {
            Console.WriteLine(value);
        }
    }
}