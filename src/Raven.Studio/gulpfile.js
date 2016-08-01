/// <binding BeforeBuild='parse-handlers, generate-typings' AfterBuild='compile' ProjectOpened='tsd, bower' />
var gulp = require('gulp'),
    gulpLoadPlugins = require('gulp-load-plugins'),
    plugins = gulpLoadPlugins(),
    fileExists = require('file-exists'),
    del = require('del'),
    Map = require('es6-map'),
    glob = require('glob'),
    fs = require('fs'),
    path = require('path'),
    runSequence = require('run-sequence'),
    exec = require('child_process').exec,
    parseHandlers = require('./gulp/parseHandlers');

var gutil = require('gulp-util');

var paths = {
    handlersToParse: [
        '../Raven.Server/**/*Handler.cs'
    ],
    handlersConstantsTargetDir: './typescript/',
    typingsConfig: './typings.json',
    tsSource: './typescript/**/*.ts',
    typings: './typings/**/*.d.ts',
    tsOutput: './wwwroot/App/',
    tsTestSource: '../../test/Studio/typescript/**/*.ts',
    tsTestOutput: '../../test/Studio/js/',
    lessSource: [
        './wwwroot/Content/app.less',
        './wwwroot/Content/bootstrap.less',
        './wwwroot/Content/dynatree.custom.less',
        './wwwroot/Content/awesome-bootstrap-checkbox.less'],
    lessTarget: './wwwroot/Content/',
    lessTargetSelector: './wwwroot/Content/**/*.css',


    oldLessSource: [
        './wwwroot/Content_old/old_app.less',
        './wwwroot/Content_old/old_bootstrap.less',
        './wwwroot/Content_old/dynatree.custom.less',
        './wwwroot/Content_old/awesome-bootstrap-checkbox.less'],
    oldLessTarget: './wwwroot/Content_old/',
    oldLessTargetSelector: './wwwroot/Content_old/**/*.css',

    releaseTarget: './build/',
    bowerSource: './wwwroot/lib/',
    cssToMerge: [
        'wwwroot/Content/bootstrap.css',
        'wwwroot/lib/eonasdan-bootstrap-datetimepicker/build/css/bootstrap-datetimepicker.css',
        'wwwroot/lib/bootstrap-select/dist/css/bootstrap-select.css',
        'wwwroot/lib/bootstrap-multiselect/dist/css/bootstrap-multiselect.css',
        'wwwroot/lib/font-awesome/css/font-awesome.css',
        'wwwroot/lib/Durandal/css/durandal.css',
        'wwwroot/lib/nprogress/nprogress.css',
        'wwwroot/lib/jquery-ui/themes/base-wo-comments/all.css',
        'wwwroot/lib/jquery.dynatree/dist/skin/ui.dynatree.css',
        'wwwroot/Content/dynatree.custom.css',
        'wwwroot/lib/nvd3/build/nv.d3.css',
        'wwwroot/Content/app.css',
        'wwwroot/lib/animate.css/animate.css'
    ],
    externalLibs: [
        'jquery/dist/jquery.min.js',
        'blockUI/jquery.blockUI.js',
        'nprogress/nprogress.js',
        'knockout/dist/knockout.js',
        'knockout.dirtyFlag/index.js',
        'knockout-delegatedEvents/build/knockout-delegatedEvents.min.js',
        'knockout-postbox/build/knockout-postbox.min.js',
        'moment/min/moment.min.js',
        'bootstrap/dist/js/bootstrap.min.js',
        'eonasdan-bootstrap-datetimepicker/build/js/bootstrap-datetimepicker.min.js',
        'bootstrap-contextmenu/bootstrap-contextmenu.js',
        'bootstrap-multiselect/dist/js/bootstrap-multiselect.js',
        'bootstrap-select/dist/js/bootstrap-select.min.js',
        'jquery-ui/ui/core.js',
        'jquery-ui/ui/widget.js',
        'jquery-ui/ui/mouse.js',
        'jquery-ui/ui/resizable.js',
        'jquery.dynatree/dist/jquery.dynatree.min.js',
        'jwerty/jwerty.js',
        'jquery-fullscreen/jquery.fullscreen-min.js',
        'spin.js/spin.min.js'
    ]
};

var tsCompilerConfig = plugins.typescript.createProject('tsconfig.json', {
    typescript: require("typescript")
});

gulp.task('clean', function () {
    del.sync(paths.releaseTarget);
    del.sync(['./typings/*', '!./typings/_studio/**', '!./typings/tsd.d.ts']);
    del.sync([paths.bowerSource]);
    del.sync(['./wwwroot/App/**/*.js']);
    del.sync(['./wwwroot/App/**/*.js.map']);
});

var newestFileFinder = function (targetGlob) {
    return function (projectDir, srcFile, absSrcFile) {
        // find newest file based on *targetToScan* and return this and file to compare against
        var files = glob.sync(targetGlob);
        var newestFile = null;
        var newestFileTimestamp = null;

        files.forEach(function (file) {
            var stats = fs.statSync(file);
            var mtime = stats.mtime.getTime();

            if (newestFileTimestamp == null || mtime > newestFileTimestamp) {
                newestFileTimestamp = mtime;
                newestFile = file;
            }
        });
        return newestFile;
    }
}

gulp.task('parse-handlers',
    function() {
        return gulp.src(paths.handlersToParse)
            .pipe(parseHandlers('endpoints.ts'))
            .pipe(gulp.dest(paths.handlersConstantsTargetDir));
    });

gulp.task('old-less', function () {
    return gulp.src(paths.oldLessSource)
        .pipe(plugins.newy(newestFileFinder(paths.oldLessTargetSelector)))
        .pipe(plugins.sourcemaps.init())
        .pipe(plugins.less({
            sourceMap: true
        }))
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(paths.oldLessTarget));
});

gulp.task('less', function() {
    return gulp.src(paths.lessSource)
        .pipe(plugins.newy(newestFileFinder(paths.lessTargetSelector)))
        .pipe(plugins.sourcemaps.init())
        .pipe(plugins.less({
            sourceMap: true
        }))
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(paths.lessTarget));
});

gulp.task('generate-typings', function(cb) {
    exec('dotnet ../../tools/TypingsGenerator/bin/Debug/netcoreapp1.0/TypingsGenerator.dll', function (err, stdout, stderr) {
        console.log(stdout);
        console.log(stderr);
        cb(err);
    });
});

gulp.task('ts-test-compile', ['parse-handlers', 'generate-typings'], function() {
     return gulp.src([paths.tsTestSource])
        .pipe(plugins.changed(paths.tsTestOutput, { extension: '.js' }))
        .pipe(plugins.sourcemaps.init())
        .pipe(plugins.typescript(tsCompilerConfig))
        .js
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(paths.tsTestOutput));
});

gulp.task('ts-compile', ['parse-handlers', 'generate-typings'], function () {
    return gulp.src([paths.tsSource])
        .pipe(plugins.changed(paths.tsOutput, { extension: '.js' }))
        .pipe(plugins.sourcemaps.init())
        .pipe(plugins.typescript(tsCompilerConfig))
        .js
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(paths.tsOutput));
});

gulp.task('typings', function() {
    return gulp.src(paths.typingsConfig)
        .pipe(plugins.typings());
});

gulp.task('bower', function () {
    return plugins.bower();
});

gulp.task('compile', ['less', 'old-less', 'ts-compile'], function() {
    // await dependent tasks
});

gulp.task('watch', ['ts-compile'], function () {
    gulp.watch(paths.tsSource, ['ts-compile']);
    gulp.watch(paths.lessSource, ['less']);
});

gulp.task('release-copy-favicon', function() {
    return gulp.src("wwwroot/favicon.ico")
        .pipe(gulp.dest(paths.releaseTarget));
});

gulp.task('release-copy-images', function() {
    return gulp.src('wwwroot/Content/images/*')
       .pipe(gulp.dest(paths.releaseTarget + "Content/images/"));
});

gulp.task('release-copy-fonts', function() {
    return gulp.src('wwwroot/fonts/*')
       .pipe(gulp.dest(paths.releaseTarget + "fonts"));
});

gulp.task('release-process-index', function() {
    return gulp.src('wwwroot/index.html')
        .pipe(plugins.processhtml())
        .pipe(gulp.dest(paths.releaseTarget));
});

/**
 * Due to https://github.com/mariocasciaro/gulp-concat-css/issues/26 we have to process jquery and remove comments 
 * to enable parsing 
 */
gulp.task('fix-jquery-ui', function() {
    return gulp.src('./wwwroot/lib/jquery-ui/themes/base/**/*.css')
        .pipe(plugins.stripCssComments())
        .pipe(gulp.dest("./wwwroot/lib/jquery-ui/themes/base-wo-comments/"));
});

gulp.task('release-process-css', ['fix-jquery-ui'], function () {
    for (var i = 0; i < paths.cssToMerge.length; i++) {
        if (!fileExists(paths.cssToMerge[i])) {
            throw new Error("Unable to find file: " + paths.cssToMerge[i]);
        }
    }

    return gulp.src(paths.cssToMerge)
        .pipe(plugins.concatCss('styles.css', { rebaseUrls: false }))
        .pipe(plugins.cssnano())
        .pipe(gulp.dest(paths.releaseTarget + "App"));
});

gulp.task('release-process-ext-js', function() {
    var externalLibs = paths.externalLibs.map(function (x) { return paths.bowerSource + x; });

    for (var i = 0; i < externalLibs.length; i++) {
        if (!fileExists(externalLibs[i])) {
            throw new Error("Unable to find file: " + externalLibs[i]);
        }
    }

    return gulp.src(externalLibs)
        .pipe(plugins.concat('external-libs.js'))
        .pipe(plugins.uglify())
        .pipe(gulp.dest(paths.releaseTarget + "App"));
});

gulp.task('release-durandal', function() {
    return plugins.durandal({
        baseDir: 'wwwroot/App',
        extraModules: ['transitions/fadeIn', 'widgets/virtualTable/viewmodel'],
        almond: true,
        minify: true,
        rjsConfigAdapter: function (cfg) {
            cfg.generateSourceMaps = false;
            return cfg;
        }
    })
   .pipe(gulp.dest(paths.releaseTarget + 'App'));
});

gulp.task('build', function (callback) {
    return runSequence('clean',
      ['bower', 'typings'],
      ['less', 'ts-compile'],
      ['release-copy-favicon', 'release-copy-images', 'release-copy-fonts', 'release-process-index', 'release-process-css', 'release-process-ext-js', 'release-durandal'],
      callback);
});