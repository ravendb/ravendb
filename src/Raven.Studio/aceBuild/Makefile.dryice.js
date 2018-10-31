#!/usr/bin/env node
/* ***** BEGIN LICENSE BLOCK *****
 * Distributed under the BSD license:
 *
 * Copyright (c) 2010, Ajax.org B.V.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Ajax.org B.V. nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL AJAX.ORG B.V. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * ***** END LICENSE BLOCK ***** */

var fs = require("fs");
var path = require("path");
var copy = require('architect-build/copy');
var build = require('architect-build/build');
var copydir = require('copy-dir');

var ACE_SOURCES = __dirname + "/node_modules/ace";
var ACE_CUSTOMAZATIONS = __dirname + "/addons";

var ACE_HOME = __dirname + "/build";

var BUILD_DIR = __dirname + "/../wwwroot/content/ace/"; 
var CACHE = {};

function main(args) {
	mergeAceWithCustomFiles();
	
	buildAce({
            compress: false, //TODO: use true
            noconflict: true,
            shrinkwrap: false
        });
}

function mergeAceWithCustomFiles() {
	// copy ACE_SOURCES to build folder
	console.log("Copying " + ACE_SOURCES + " to " + ACE_HOME);
	copydir.sync(ACE_SOURCES, ACE_HOME);
	
	console.log("Copying " + ACE_CUSTOMAZATIONS + " to " + ACE_HOME);
	copydir.sync(ACE_CUSTOMAZATIONS, ACE_HOME);
}

function jsFileList(path, filter) {
    path = ACE_HOME + "/" + path;
    if (!filter)
        filter = /_test/;

    return fs.readdirSync(path).map(function(x) {
        if (x.slice(-3) == ".js" && !filter.test(x) && !/\s|BASE|(\b|_)dummy(\b|_)/.test(x))
            return x.slice(0, -3);
    }).filter(Boolean);
}

function workers() {
	return ["javascript", "json", "rql"];
}

function modeList() {
	return ["csharp", "javascript", "json", "json_newline_friendly", "lucene", 
			"mysql", "ravenMapLinq", "ravenReduceLinq", "rql", "sql", "sqlserver", "text"];
}

function buildAceModule(opts, callback) {
    // calling buildAceModuleInternal many times in parallel is slow, so we use queue
    if (!buildAceModule.queue) {
        buildAceModule.queue = [];
        buildAceModule.dequeue = function() {
            if (buildAceModule.running) return;
            var call = buildAceModule.queue.shift();
            buildAceModule.running = call;
            if (call)
                buildAceModuleInternal.apply(null, call);
        };
    }
    
    buildAceModule.queue.push([opts, function(err, result) {
        callback && callback(err, result);
        buildAceModule.running = null;
        buildAceModule.dequeue();
    }]);

    if (!buildAceModule.running) {
        buildAceModule.dequeue();
    } else {
        process.nextTick(buildAceModule.dequeue);
    }
}

function buildAceModuleInternal(opts, callback) {
    var cache = opts.cache == undefined ? CACHE : opts.cache;
    var key = opts.require + "|" + opts.projectType;
    if (cache && cache.configs && cache.configs[key])
        return write(null, cache.configs[key]);
        
    var pathConfig = {
        paths: {
            ace: ACE_HOME + "/lib/ace",
            "kitchen-sink": ACE_HOME + "/demo/kitchen-sink",
            build_support:  ACE_HOME + "/build_support"
        },
        root: ACE_HOME
    };
        
    function write(err, result) {
        if (cache && key && !(cache.configs && cache.configs[key])) {
            cache.configs = cache.configs || Object.create(null);
            cache.configs[key] = result;
            result.sources = result.sources.map(function(pkg) {
                return {deps: pkg.deps};
            });
        } 
        
        if (!opts.outputFile)
            return callback(err, result);
        
        var code = result.code;
        if (opts.compress) {
            if (!result.codeMin)
                result.codeMin = compress(result.code);
            code = result.codeMin;
        }
            
        var targetDir = getTargetDir(opts);
        
        var to = /^([\\/]|\w:)/.test(opts.outputFile)
            ? opts.outputFile
            : path.join(opts.outputFolder || targetDir, opts.outputFile);
    
        var filters = [];

        var ns = opts.ns || "ace";
        if (opts.filters)
            filters = filters.concat(opts.filters);
    
        if (opts.noconflict)
            filters.push(namespace(ns));
        var projectType = opts.projectType;
        if (projectType == "main" || projectType == "ext") {
            filters.push(exportAce(ns, opts.require[0],
                opts.noconflict ? ns : "", projectType == "ext"));
        }
        
        filters.push(normalizeLineEndings);
        
        filters.forEach(function(f) { code = f(code); });
        
        build.writeToFile({code: code}, {
            outputFolder: path.dirname(to),
            outputFile: path.basename(to)
        }, function() {});
        
        callback && callback(err, result);
    }
    
    build(opts.require, {
        cache: cache,
        quiet: opts.quiet,
        pathConfig: pathConfig,
        additional: opts.additional,
        enableBrowser: true,
        keepDepArrays: "all",
        noArchitect: true,
        compress: false,
        ignore: opts.ignore || [],
        withRequire: false,
        basepath: ACE_HOME,
        transforms: [normalizeLineEndings],
        afterRead: [optimizeTextModules]
    }, write);
}

function buildCore(options, extra, callback) {
    options = extend(extra, options);
    options.additional = [{
        id: "build_support/mini_require", 
        order: -1000,
        literal: true
    }];
    options.require =["ace/ace"];
    options.projectType = "main";
    options.ns = "ace";
    buildAceModule(options, callback);
}

function buildSubmodule(options, extra, file, callback) {
    options = extend(extra, options);
    getLoadedFileList(options, function(coreFiles) {
        options.outputFile = file + ".js";
        options.ignore = options.ignore || coreFiles;
        options.quiet = true;
        buildAceModule(options, callback);
    });
}

function buildAce(options, callback) {
    var modeNames = modeList();

    buildCore(options, {outputFile: "ace.js"}, addCb());
    // modes
    modeNames.forEach(function(name) {
        buildSubmodule(options, {
            projectType: "mode",
            require: ["ace/mode/" + name]
        }, "mode-" + name, addCb());
    });
   		
    // themes
    ["ambiance", "pastel_dark_raven", "pastel_on_dark"].forEach(function(name) {
        buildSubmodule(options, {
            projectType: "theme",
            require: ["ace/theme/" + name]
        }, "theme-" +  name, addCb());
    });
    
    // extensions
    ["searchbox", "language_tools"].forEach(function(name) {
        buildSubmodule(options, {
            projectType: "ext",
            require: ["ace/ext/" + name]
        }, "ext-" + name, addCb());
    });
    // workers
    workers().forEach(function(name) {
        buildSubmodule(options, {
            projectType: "worker",
            require: ["ace/mode/" + name + "_worker"],
            ignore: [],
            additional: [{
                id: "ace/worker/worker",
                transforms: [],
                order: -1000
            }]
        }, "worker-" + name, addCb());
    });
    // 
    function addCb() {
        addCb.count = (addCb.count || 0) + 1; 
        return done
    }
    function done() {
        if (--addCb.count > 0)
            return;
        if (options.check)
            sanityCheck(options, callback);
            
        if (callback) 
            return callback();
        console.log("Finished building " + getTargetDir(options))
    }
}

function getLoadedFileList(options, callback, result) {
    if (!result) {
        return buildCore({}, {}, function(e, result) {
            getLoadedFileList(options, callback, result);
        });
    }
    var deps = Object.create(null);
	
    result.sources.forEach(function(pkg) {
        pkg.deps && pkg.deps.forEach(function(p) {
            if (!deps[p]) deps[p] = 1;
        });
    });
    delete deps["ace/theme/textmate"];
    deps["ace/ace"] = 1;
    callback(Object.keys(deps));
}

function normalizeLineEndings(module) {
    if (typeof module == "string") 
        module = {source: module};
    return module.source = module.source.replace(/\r\n/g, "\n");
}

function optimizeTextModules(sources) {
    var textModules = {};
    return sources.filter(function(pkg) {
        if (!pkg.id) {
            return true;
        }
        else if (pkg.id.indexOf("text!") > -1) {
            detectTextModules(pkg);
            return false;
        }
        else {
            pkg.source = rewriteTextImports(pkg.source, pkg.deps);
            return true;
        }
    }).map(function(pkg) {
        if (pkg && pkg.deps) {
            pkg.deps = pkg.deps && pkg.deps.filter(function(p) {
                return p.indexOf("text!") == -1;
            });
        }
        return pkg;
    });
    
    function rewriteTextImports(text, deps) {
        return text.replace(/ require\(['"](?:ace|[.\/]+)\/requirejs\/text!(.*?)['"]\)/g, function(_, call) {
            if (call) {
                var dep;
                deps.some(function(d) {
                    if (d.split("/").pop() == call.split("/").pop()) {
                        dep = d;
                        return true;
                    }
                });
    
                call = textModules[dep];
                if (call)
                    return " " + call;
            }
        });
    }
    function detectTextModules(pkg) {
        var input = pkg.source.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
        if (/\.css$/.test(pkg.id)) {
            // remove unnecessary whitespace from css
            input = input.replace(/\n\s+/g, "\n");
            input = '"' + input.replace(/\n/g, '\\\n') + '"';
        } else {
            // but don't break other files!
            input = '"' + input.replace(/\n/g, '\\n\\\n') + '"';
        }
        textModules[pkg.id] = input;
    }
}

function namespace(ns) {
    return function(text) {
        text = text
            .toString()
            .replace(/ACE_NAMESPACE\s*=\s*""/, 'ACE_NAMESPACE = "' + ns +'"')
            .replace(/\bdefine\(/g, function(def, index, source) {
                if (/(^|[;}),])\s*$/.test(source.slice(0, index)))
                    return ns + "." + def;
                return def;
            });

        return text;
    };
}

function exportAce(ns, modules, requireBase, extModules) {
    requireBase = requireBase || "window";
    return function(text) {
        /*globals REQUIRE_NS, MODULES, NS*/
        var template = function() {
            (function() {
                REQUIRE_NS.require(MODULES, function(a) {
                    if (a) {
                        a.config.init(true);
                        a.define = REQUIRE_NS.define;
                    }
                    if (!window.NS)
                        window.NS = a;
                    for (var key in a) if (a.hasOwnProperty(key))
                        window.NS[key] = a[key];
                });
            })();
        };
        
        if (extModules) {
            template = function() {
                (function() {
                    REQUIRE_NS.require(MODULES, function() {});
                })();
            };
        }
        
        text = text.replace(/function init\(packaged\) {/, "init(true);$&\n");
        
        if (typeof modules == "string")
            modules = [modules];
            
        return (text.replace(/;\s*$/, "") + ";" + template
            .toString()
            .replace(/MODULES/g, JSON.stringify(modules))
            .replace(/REQUIRE_NS/g, requireBase)
            .replace(/NS/g, ns)
            .slice(13, -1)
        );
    };
}


function compress(text) {
    var ujs = require("dryice").copy.filter.uglifyjs;
    ujs.options.mangle_toplevel = {except: ["ACE_NAMESPACE", "requirejs"]};
    ujs.options.beautify = {ascii_only: true, inline_script: true}
    return ujs(text);
}

function extend(base, extra) {
    Object.keys(extra).forEach(function(k) {
        base[k] = extra[k];
    });
    return base;
}

function getTargetDir(opts) {
	return BUILD_DIR;
}

function sanityCheck(opts, callback) {
    var targetDir = getTargetDir(opts);
    require("child_process").execFile(process.execPath, ["-e", "(" + function() {
        window = global;
        require("./ace");
        if (typeof ace.edit != "function")
            process.exit(1);
        require("fs").readdirSync(".").forEach(function(p) {
            if (!/ace\.js$/.test(p) && /\.js$/.test(p))
                require("./" + p);
        });
        process.exit(0);
    } + ")()"], {
        cwd: targetDir
    }, function(err, stdout) {
        if (callback) return callback(err, stdout);
        if (err)
            throw err;
    });
}

if (!module.parent)
    main(process.argv);
else
    exports.buildAce = buildAce;
