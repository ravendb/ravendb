/// <binding BeforeBuild='generate-ts' AfterBuild='compile-changed:app' ProjectOpened='restore' />

require('./gulp/shim');

var gulp = require('gulp'),
    gulpLoadPlugins = require('gulp-load-plugins'),
    plugins = gulpLoadPlugins(),
    path = require('path'),
    del = require('del'),
    Map = require('es6-map'),
    runSequence = require('run-sequence'),
    exec = require('child_process').exec,
    parseHandlers = require('./gulp/parseHandlers'),
    parseConfiguration = require('./gulp/parseConfiguration'),
    findNewestFile = require('./gulp/findNewestFile'),
    checkAllFilesExist = require('./gulp/checkAllFilesExist'),
    gutil = require('gulp-util');

var PATHS = require('./gulp/paths');

var tsCompilerConfig = plugins.typescript.createProject('tsconfig.json', {
    typescript: require('typescript')
});

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
        .pipe(plugins.less({
            sourceMap: true
        }))
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.lessTarget));
});

gulp.task('generate-typings', function(cb) {
    exec('dotnet ../../tools/TypingsGenerator/bin/Debug/netcoreapp1.0/TypingsGenerator.dll', function (err, stdout, stderr) {
        console.log(stdout);
        console.log(stderr);
        cb(err);
    });
});

gulp.task('compile:test', ['generate-ts'], function() {
     return gulp.src([PATHS.test.tsSource])
        .pipe(plugins.sourcemaps.init())
        .pipe(plugins.typescript(tsCompilerConfig))
        .js
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.test.tsOutput));
});

gulp.task('compile:app', ['generate-ts'], function () {
    return gulp.src([PATHS.tsSource])
        .pipe(plugins.sourcemaps.init())
        .pipe(plugins.typescript(tsCompilerConfig))
        .js
        .pipe(plugins.sourcemaps.write("."))
        .pipe(gulp.dest(PATHS.tsOutput));
});

gulp.task('compile-changed:app', ['generate-ts'], function() {
    return gulp.src([PATHS.tsSource])
        .pipe(plugins.changed(PATHS.tsOutput, { extension: '.js' }))
        .pipe(plugins.sourcemaps.init())
        .pipe(plugins.typescript(tsCompilerConfig))
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

gulp.task('release:images', function() {
    return gulp.src('wwwroot/Content/images/*')
       .pipe(gulp.dest(PATHS.releaseTarget + "Content/images/"));
});

gulp.task('release:fonts', function() {
    return gulp.src('wwwroot/Content/css/fonts/**/*')
       .pipe(gulp.dest(path.join(PATHS.releaseTarget, 'App/fonts')));
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
        .pipe(gulp.dest(PATHS.releaseTargetApp));
});

gulp.task('release:libs', function() {
    var externalLibs = PATHS.externalLibs.map(function (x) { return PATHS.bowerSource + x; });
    checkAllFilesExist(externalLibs);

    return gulp.src(externalLibs)
        .pipe(plugins.concat('external-libs.js'))
        .pipe(plugins.uglify())
        .pipe(gulp.dest(PATHS.releaseTargetApp));
});

gulp.task('release:durandal', function() {
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
        reporter: 'spec',
        dump: 'test.log'
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
    gulp.watch(PATHS.tsSource, ['compile:app']);
    gulp.watch(PATHS.test.tsSource, ['compile:test']);
    gulp.watch(PATHS.lessSource, ['less']);
});

gulp.task('generate-ts', ['parse-handlers', 'parse-configuration', 'generate-typings'], function() {});

gulp.task('restore', ['bower', 'typings']);

gulp.task('release', function (cb) {
    return runSequence(
        'clean',
        'build',
        [
            'release:libs',
            'release:favicon',
            'release:images',
            'release:html',
            'release:css',
            'release:fonts',
            'release:durandal'
        ],
        cb);
});

gulp.task('build', function (cb) {
    return runSequence(
        'restore',
        'compile',
        cb);
});
