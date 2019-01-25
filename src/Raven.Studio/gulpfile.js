/// <binding />

require('./gulp/shim');

var gulp = require('gulp'),
    gulpLoadPlugins = require('gulp-load-plugins'),
    plugins = gulpLoadPlugins(),
    path = require('path'),
    fs = require('fs'),
    del = require('del'),
    Map = require('es6-map'),
    runSequence = require('run-sequence'),
    exec = require('child_process').exec,
    parseHandlers = require('./gulp/parseHandlers'),
    parseConfiguration = require('./gulp/parseConfiguration'),
    findNewestFile = require('./gulp/findNewestFile'),
    checkAllFilesExist = require('./gulp/checkAllFilesExist'),
    gutil = require('gulp-util'),
    cachebust = require('gulp-cache-bust'),
    autoPrefixer = require('gulp-autoprefixer'),
    fileExists = require('file-exists'),
    fsUtils = require('./gulp/fsUtils');

var PATHS = require('./gulp/paths');

var tsProject = plugins.typescript.createProject('tsconfig.json');
var testTsProject = plugins.typescript.createProject('tsconfig.json');

gulp.task('z_clean', ['z_clean:js'], function () {
    del.sync(PATHS.releaseTarget);
    del.sync(['./typings/*', '!./typings/_studio/**', '!./typings/tsd.d.ts']);
    del.sync([PATHS.bowerSource]);
});

gulp.task('z_clean:js', function() {
    del.sync(['./wwwroot/App/**/*.js']);
    del.sync(['./wwwroot/App/**/*.js.map']);
});

gulp.task('z_parse-handlers', function() {
    return gulp.src(PATHS.handlersToParse)
        .pipe(parseHandlers('endpoints.ts'))
        .pipe(gulp.dest(PATHS.constantsTargetDir));
});

gulp.task('z_parse-configuration', function() {
    return gulp.src(PATHS.configurationFilesToParse)
        .pipe(parseConfiguration('configuration.ts'))
        .pipe(gulp.dest(PATHS.constantsTargetDir));
});

gulp.task('less', function() {
    return gulp.src(PATHS.lessSource, { base: './wwwroot/Content/' })
        .pipe(plugins.sourcemaps.init())
        .pipe(plugins.less({ sourceMap: true }))
        .pipe(autoPrefixer())
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.lessTarget));
});

gulp.task('z_generate-typings', function (cb) {
    var possibleTypingsGenPaths = [
        '../../tools/TypingsGenerator/bin/Debug/netcoreapp2.2/TypingsGenerator.dll',
        '../../tools/TypingsGenerator/bin/Release/netcoreapp2.2/TypingsGenerator.dll' ];

    var dllPath = fsUtils.getLastRecentlyModifiedFile(possibleTypingsGenPaths);
    if (!dllPath) {
        cb(new Error('TypingsGenerator.dll not found neither for Release nor Debug directory.'));
        return;
    }

    gutil.log(`Running: dotnet ${dllPath}`);
    exec('dotnet ' + dllPath, function (err, stdout, stderr) {
        if (err) {
            gutil.log(stdout);
            gutil.log(stderr);
        }

        cb(err);
    });
});

gulp.task('z_compile:test', ['z_generate-ts'], function() {
     return gulp.src([PATHS.test.tsSource])
        .pipe(plugins.naturalSort())
        .pipe(plugins.sourcemaps.init())
        .pipe(testTsProject())
        .js
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.test.tsOutput));
});

gulp.task('z_compile:app', ['z_generate-ts'], function () {
    return gulp.src([PATHS.tsSource])
        .pipe(plugins.naturalSort())
        .pipe(plugins.sourcemaps.init())
        .pipe(tsProject())
        .js
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.tsOutput));
});

gulp.task('z_compile:app-changed', [], function () {
    return gulp.src([PATHS.tsSource])
        .pipe(plugins.changed(PATHS.tsOutput, { extension: '.js' }))
        .pipe(plugins.sourcemaps.init())
        .pipe(tsProject())
        .js
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.tsOutput));
});

gulp.task('z_typings', function() {
    return gulp.src(PATHS.typingsConfig)
        .pipe(plugins.typings());
});

gulp.task('z_bower', function () {
    return plugins.bower();
});

gulp.task('z_release:favicon', function() {
    return gulp.src([
            "wwwroot/android-chrome*",
            "wwwroot/apple*",
            "wwwroot/browserconfig.xml",
            "wwwroot/favicon*",
            "wwwroot/manifest.json",
            "wwwroot/mstile-*",
            "wwwroot/safari-pinned-tab*"
        ])
        .pipe(gulp.dest(PATHS.releaseTarget));
});

gulp.task('z_release:ace-workers', function () {
    return gulp.src("wwwroot/Content/ace/worker*.js")
        .pipe(gulp.dest(PATHS.releaseTarget));
});

gulp.task('z_release:images', function() {
    return gulp.src('wwwroot/Content/img/*', { base: 'wwwroot/Content' })
       .pipe(gulp.dest(PATHS.releaseTargetContent));
});

gulp.task('z_release:fonts', function() {
    return gulp.src('wwwroot/Content/css/fonts/**/*')
       .pipe(gulp.dest(path.join(PATHS.releaseTargetContentCss, 'fonts')));
});

gulp.task('z_release:html', function() {
    return gulp.src('wwwroot/index.html')
        .pipe(plugins.processhtml())
        .pipe(cachebust({ 
            type: 'timestamp'
        }))
        .pipe(gulp.dest(PATHS.releaseTarget));
});

gulp.task('z_release:css-common', function () {
    checkAllFilesExist(PATHS.cssToMerge);
    return gulp.src(PATHS.cssToMerge)
        .pipe(plugins.concatCss('styles-common.css', { rebaseUrls: false }))
        .pipe(plugins.cssnano())
        .pipe(gulp.dest(PATHS.releaseTargetContentCss));
});

gulp.task('z_release:theme-css', function () {
    checkAllFilesExist(PATHS.themeCss);
    return gulp.src(PATHS.themeCss)
        .pipe(plugins.cssnano())
        .pipe(gulp.dest(PATHS.releaseTargetContentCss));
});

gulp.task('z_release:libs', function() {
    var externalLibs = PATHS.externalLibs.map(function (x) { return PATHS.bowerSource + x; });
    checkAllFilesExist(externalLibs);

    return gulp.src(externalLibs)
        .pipe(plugins.concat('external-libs.js'))
        .pipe(plugins.uglify())
        .pipe(gulp.dest(PATHS.releaseTargetApp));
});

gulp.task('z_release:copy-version', function () {
    return gulp.src("./wwwroot/version.txt")
        .pipe(gulp.dest(PATHS.releaseTarget));
});

gulp.task('z_release:package', function () {
    return gulp.src(PATHS.releaseTarget + "/**/*.*")
    .pipe(plugins.zip("Raven.Studio.zip"))
    .pipe(gulp.dest(PATHS.releaseTarget));
});

gulp.task('z_release:durandal', function () {
    var extraModules = [
        'transitions/fadeIn',
        'ace/ace',
        '../lib/forge/js/forge.js'
    ];

    var aceFileNames = fs.readdirSync(PATHS.aceDir)
        .filter(x => x !== "." && x !== ".." && x !== "ace.js" && !x.startsWith("worker"));

    for (var i = 0; i < aceFileNames.length; i++) {
        var fileName = aceFileNames[i];
        if (fileName.endsWith(".js")) {
            var moduleName = "ace/" + path.basename(fileName, ".js");
            extraModules.push(moduleName);
        }
    }

    return plugins.durandal({
        baseDir: 'wwwroot/App',
        extraModules: extraModules,
        almond: true,
        minify: true,
        rjsConfigAdapter: function (cfg) {
            cfg.generateSourceMaps = false;
            return cfg;
        }
    })
   .pipe(plugins.insert.prepend('window.ravenStudioRelease = true;'))
   .pipe(gulp.dest(PATHS.releaseTargetApp));
});

gulp.task('z_generate-test-list', function () {
    var reduceFiles = plugins.reduceFile('tests.js',
        function (file, memo) {
            memo.push(file.relative.replace(/\\/g, '/'));
            return memo;
        }, function (memo) {
            return 'var tests = ' + JSON.stringify(memo, null , 2) + ';';
        }, [])

    return gulp.src([
        '**/*.spec.js'
    ], {
        cwd: PATHS.test.tsOutput,
        base: PATHS.test.dir
    })
    .pipe(reduceFiles)
    .pipe(gulp.dest(PATHS.test.setup));
});

gulp.task('z_mochaTests', function () {
    var mochaPhantomJs;
    
    try {
        var mochaPhantomJs = require("gulp-mocha-phantomjs");
    } catch (e) {
        throw new Error("Looks like gulp-mocha-phantomjs is missing. Please run 'npm install gulp-mocha-phantomjs --no-save' in src/Raven.Studio directory, and rerun the tests. " + e);
    }

    var mocha = mochaPhantomJs({
        reporter: 'spec', //use json for debugging,
        suppressStdout: false,
        suppressStderr: false
    });

    return gulp.src(PATHS.test.html).pipe(mocha);
});

gulp.task('test', [ 'z_compile:test' ], function (cb) {
    return runSequence('z_generate-test-list', 'z_mochaTests', cb);
});

gulp.task('generate-tests', [ 'z_compile:test' ], function (cb) {
    return runSequence('z_generate-test-list', cb);
});

gulp.task('z_watch:test', ['test'], function () {
    gulp.watch(PATHS.tsSource, ['z_mochaTests']);
    gulp.watch(PATHS.test.tsSource, ['test']);
});

gulp.task('z_watch_test', ['compile', 'generate-tests'], function () {
    gulp.watch(PATHS.watchDirectories, ['z_compile:app-changed', 'generate-tests']);
    gulp.watch(PATHS.lessSourcesToWatch, ['less']);
});

gulp.task('compile', ['less', 'z_compile:app'], function() { });

gulp.task('watch', ['compile'], function () {
    gulp.watch(PATHS.tsSource, ['z_compile:app-changed']);
    gulp.watch(PATHS.test.tsSource, ['z_compile:test']);
    gulp.watch(PATHS.lessSourcesToWatch, ['less']);
});

gulp.task('z_generate-ts', [
    'z_parse-handlers',
    'z_parse-configuration',
    'z_generate-typings'],
    function() {});

gulp.task('restore', [
    'z_bower',
    'z_typings'
]);

gulp.task('release', function (cb) {
    return runSequence(
        'z_clean',
        'restore+compile',
        [
            'z_release:libs',
            'z_release:copy-version',
            'z_release:favicon',
            'z_release:ace-workers',
            'z_release:images',
            'z_release:html',
            'z_release:css-common',
            'z_release:theme-css',
            'z_release:fonts',
            'z_release:durandal'
        ],
        'z_release:package',
        cb);
});

gulp.task('restore+compile', function (cb) {
    return runSequence(
        'restore',
        'compile',
        cb);
});
