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
    autoPrefixer = require('gulp-autoprefixer'),
    fileExists = require('file-exists'),
    fsUtils = require('./gulp/fsUtils');

var PATHS = require('./gulp/paths');

var tsProject = plugins.typescript.createProject('tsconfig.json');

gulp.task('clean', ['clean:js'], function () {
    del.sync(PATHS.releaseTarget);
    del.sync(['./typings/*', '!./typings/_studio/**', '!./typings/tsd.d.ts']);
    del.sync([PATHS.bowerSource]);
});

gulp.task('clean:js', function() {
    del.sync(['./wwwroot/App/**/*.js']);
    del.sync(['./wwwroot/App/**/*.js.map']);
});

gulp.task('parse-handlers', function() {
    return gulp.src(PATHS.handlersToParse)
        .pipe(parseHandlers('endpoints.ts'))
        .pipe(gulp.dest(PATHS.constantsTargetDir));
});

gulp.task('parse-configuration', function() {
    return gulp.src(PATHS.configurationFilesToParse)
        .pipe(parseConfiguration('configuration.ts'))
        .pipe(gulp.dest(PATHS.constantsTargetDir));
});

gulp.task('less', function() {
    return gulp.src(PATHS.lessSource, { base: './wwwroot/Content/' })
         // .pipe(plugins.newy(findNewestFile(PATHS.lessTargetSelector)))
        .pipe(plugins.sourcemaps.init())
        .pipe(plugins.less({ sourceMap: true }))
        .pipe(autoPrefixer())
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.lessTarget));
});

gulp.task('generate-typings', function (cb) {
    var possibleTypingsGenPaths = [
        '../../tools/TypingsGenerator/bin/Debug/netcoreapp1.1/TypingsGenerator.dll',
        '../../tools/TypingsGenerator/bin/Release/netcoreapp1.1/TypingsGenerator.dll' ];

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

gulp.task('compile:test', ['generate-ts'], function() {
     return gulp.src([PATHS.test.tsSource])
        .pipe(plugins.sourcemaps.init())
        .pipe(tsProject())
        .js
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.test.tsOutput));
});

gulp.task('compile:app', ['generate-ts'], function () {
    return gulp.src([PATHS.tsSource])
        .pipe(plugins.naturalSort())
        .pipe(plugins.sourcemaps.init())
        .pipe(tsProject())
        .js
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.tsOutput));
});

gulp.task('compile:app-changed', [], function () {
    return gulp.src([PATHS.tsSource])
        .pipe(plugins.changed(PATHS.tsOutput, { extension: '.js' }))
        .pipe(plugins.sourcemaps.init())
        .pipe(tsProject())
        .js
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.tsOutput));
});

gulp.task('typings', function() {
    return gulp.src(PATHS.typingsConfig)
        .pipe(plugins.typings());
});

gulp.task('bower', function () {
    return plugins.bower();
});

gulp.task('release:favicon', function() {
    return gulp.src("wwwroot/favicon.ico")
        .pipe(gulp.dest(PATHS.releaseTarget));
});

gulp.task('release:ace-workers', function () {
    return gulp.src("wwwroot/Content/ace/worker*.js")
        .pipe(gulp.dest(PATHS.releaseTarget));
});

gulp.task('release:images', function() {
    return gulp.src('wwwroot/Content/img/*', { base: 'wwwroot/Content' })
       .pipe(gulp.dest(PATHS.releaseTargetContent));
});

gulp.task('release:fonts', function() {
    return gulp.src('wwwroot/Content/css/fonts/**/*')
       .pipe(gulp.dest(path.join(PATHS.releaseTargetContentCss, 'fonts')));
});

//TODO: delete this task once we remove font awesome
gulp.task('release:temp-font-awesome', function () {
    return gulp.src('wwwroot/lib/font-awesome/fonts/**/*')
       .pipe(gulp.dest(path.join(PATHS.releaseTargetContent, 'fonts')));
});

gulp.task('release:html', function() {
    return gulp.src('wwwroot/index.html')
        .pipe(plugins.processhtml())
        .pipe(gulp.dest(PATHS.releaseTarget));
});

gulp.task('fix-jquery-ui', function() {
    /*
    * Due to https://github.com/mariocasciaro/gulp-concat-css/issues/26 we have to process jquery and remove comments
    * to enable parsing
    */
    return gulp.src('./wwwroot/lib/jquery-ui/themes/base/**/*.css')
        .pipe(plugins.stripCssComments())
        .pipe(gulp.dest("./wwwroot/lib/jquery-ui/themes/base-wo-comments/"));
});

gulp.task('release:css', ['fix-jquery-ui'], function () {
    checkAllFilesExist(PATHS.cssToMerge);
    return gulp.src(PATHS.cssToMerge)
        .pipe(plugins.concatCss('styles.css', { rebaseUrls: false }))
        .pipe(plugins.cssnano())
        .pipe(gulp.dest(PATHS.releaseTargetContentCss));
});

gulp.task('release:libs', function() {
    var externalLibs = PATHS.externalLibs.map(function (x) { return PATHS.bowerSource + x; });
    checkAllFilesExist(externalLibs);

    return gulp.src(externalLibs)
        .pipe(plugins.concat('external-libs.js'))
        .pipe(plugins.uglify())
        .pipe(gulp.dest(PATHS.releaseTargetApp));
});

gulp.task('release:copy-version', function () {
    return gulp.src("./version.json")
        .pipe(gulp.dest(PATHS.releaseTarget));
});

gulp.task('release:package', function () {
    return gulp.src(PATHS.releaseTarget + "/**/*.*")
    .pipe(plugins.zip("Raven.Studio.zip"))
    .pipe(gulp.dest(PATHS.releaseTarget));
});

gulp.task('release:durandal', function () {
    var extraModules = [
        'transitions/fadeIn',
        'widgets/virtualTable/viewmodel'
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
   .pipe(gulp.dest(PATHS.releaseTargetApp));
});

gulp.task('generate-test-list', function () {
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

gulp.task('mochaTests', function () {
    var mocha = plugins.mochaPhantomjs({
        reporter: 'spec' //use json for debugging
    });

    return gulp.src(PATHS.test.html).pipe(mocha);
});

gulp.task('test', [ 'compile:test' ], function (cb) {
    return runSequence('generate-test-list', 'mochaTests', cb);
});

gulp.task('watch:test', ['test'], function () {
    gulp.watch(PATHS.tsSource, ['mochaTests']);
    gulp.watch(PATHS.test.tsSource, ['test']);
});

gulp.task('compile', ['less', 'compile:app'], function() { });

gulp.task('watch', ['compile'], function () {
    gulp.watch(PATHS.tsSource, ['compile:app-changed']);
    gulp.watch(PATHS.test.tsSource, ['compile:test']);
    gulp.watch(PATHS.lessSourcesToWatch, ['less']);
});

gulp.task('generate-ts', ['parse-handlers', 'parse-configuration', 'generate-typings'], function() {});

gulp.task('restore', ['bower', 'typings']);

gulp.task('release', function (cb) {
    return runSequence(
        'clean',
        'build',
        [
            'release:libs',
            'release:copy-version',
            'release:favicon',
            'release:ace-workers',
            'release:images',
            'release:html',
            'release:css',
            'release:fonts',
            'release:temp-font-awesome', //TODO: temp fix: we won't have font-awesome in final
            'release:durandal'
        ],
        'release:package',
        cb);
});

gulp.task('build', function (cb) {
    return runSequence(
        'restore',
        'compile',
        cb);
});
