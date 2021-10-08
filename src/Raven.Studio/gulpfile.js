/// <binding />

require('./gulp/shim');

const gulp = require('gulp');
const exec = require('child_process').exec;
const parseHandlers = require('./gulp/parseHandlers');
const parseConfiguration = require('./gulp/parseConfiguration');
const checkAllFilesExist = require('./gulp/checkAllFilesExist');
const gutil = require('gulp-util');
const fsUtils = require('./gulp/fsUtils');
const cssNano = require("gulp-cssnano");
const concatCss = require("gulp-concat-css");

const PATHS = require('./gulp/paths');

function z_parse_handlers() {
    return gulp.src(PATHS.handlersToParse)
        .pipe(parseHandlers('endpoints.ts'))
        .pipe(gulp.dest(PATHS.constantsTargetDir));
}
    
function z_parse_configuration() {
    return gulp.src(PATHS.configurationFilesToParse)
        .pipe(parseConfiguration('configuration.ts'))
        .pipe(gulp.dest(PATHS.constantsTargetDir));
}

function z_generate_typings(cb) {
    const possibleTypingsGenPaths = [
        '../../tools/TypingsGenerator/bin/Debug/net5.0/TypingsGenerator.dll',
        '../../tools/TypingsGenerator/bin/Release/net5.0/TypingsGenerator.dll' ];

    const dllPath = fsUtils.getLastRecentlyModifiedFile(possibleTypingsGenPaths);
    if (!dllPath) {
        cb(new Error('TypingsGenerator.dll not found neither for Release nor Debug directory.'));
        return;
    }

    gutil.log(`Running: dotnet ${dllPath}`);
    return exec('dotnet ' + dllPath, function (err, stdout, stderr) {
        if (err) {
            gutil.log(stdout);
            gutil.log(stderr);
        }

        cb(err);
    });
}

function z_release_ace_workers() {
    return gulp.src("wwwroot/Content/ace/worker*.js")
        .pipe(gulp.dest(PATHS.releaseTarget));
}

function z_release_images() {
    return gulp.src('wwwroot/Content/img/**/*', { base: 'wwwroot/Content' })
        .pipe(gulp.dest(PATHS.releaseTargetContent));
}

function z_release_fonts() {
    return gulp.src('wwwroot/Content/css/fonts/**/*')
        .pipe(gulp.dest(path.join(PATHS.releaseTargetContentCss, 'fonts')));
}

function z_release_html() {
    return gulp.src('wwwroot/index.html')
        .pipe(processHtml())
        .pipe(cachebust({
            type: 'timestamp'
        }))
        .pipe(gulp.dest(PATHS.releaseTarget));
}

function z_release_css_common() {
    checkAllFilesExist(PATHS.cssToMerge);
    return gulp.src(PATHS.cssToMerge)
        .pipe(concatCss('styles-common.css', { rebaseUrls: false }))
        .pipe(cssNano())
        .pipe(gulp.dest(PATHS.releaseTargetContentCss));
}

function z_release_theme_css() {
    checkAllFilesExist(PATHS.themeCss);
    return gulp.src(PATHS.themeCss)
        .pipe(cssNano())
        .pipe(gulp.dest(PATHS.releaseTargetContentCss));
}

function z_release_libs() {
    const externalLibs = PATHS.externalLibs.map(function (x) { return PATHS.bowerSource + x; });
    checkAllFilesExist(externalLibs);

    return gulp.src(externalLibs)
        .pipe(concat('external-libs.js'))
        .pipe(uglify())
        .pipe(gulp.dest(PATHS.releaseTargetApp));
}

function z_release_copy_version() {
    return gulp.src("./wwwroot/version.txt")
        .pipe(gulp.dest(PATHS.releaseTarget));
}

function z_release_package() {
    return gulp.src(PATHS.releaseTarget + "/**/*.*")
        .pipe(zip("Raven.Studio.zip"))
        .pipe(gulp.dest(PATHS.releaseTarget));
}

function z_release_durandal() {
    const extraModules = [
        'transitions/fadeIn',
        'ace/ace',
        '../lib/forge/js/forge.js'
    ];

    const aceFileNames = fs.readdirSync(PATHS.aceDir)
        .filter(x => x !== "." && x !== ".." && x !== "ace.js" && !x.startsWith("worker"));

    for (let i = 0; i < aceFileNames.length; i++) {
        const fileName = aceFileNames[i];
        if (fileName.endsWith(".js")) {
            const moduleName = "ace/" + path.basename(fileName, ".js");
            extraModules.push(moduleName);
        }
    }

    return durandal({
        baseDir: 'wwwroot/App',
        extraModules: extraModules,
        almond: true,
        minify: true,
        rjsConfigAdapter: function (cfg) {
            cfg.generateSourceMaps = false;
            return cfg;
        }
    })
        .pipe(insert.prepend('window.ravenStudioRelease = true;'))
        .pipe(gulp.dest(PATHS.releaseTargetApp));
}

function z_generate_test_list() {
    const reduceFiles = reduceFile('tests.js',
        function (file, memo) {
            memo.push(file.relative.replace(/\\/g, '/'));
            return memo;
        }, function (memo) {
            return 'var tests = ' + JSON.stringify(memo, null , 2) + ';';
        }, []);

    return gulp.src([
        '**/*.spec.js'
    ], {
        cwd: PATHS.test.tsOutput,
        base: PATHS.test.dir
    })
        .pipe(reduceFiles)
        .pipe(gulp.dest(PATHS.test.setup));
}

function z_mocha_tests() {
    let mochaPhantomJs;

    try {
       mochaPhantomJs = require("gulp-mocha-phantomjs");
    } catch (e) {
        throw new Error("Looks like gulp-mocha-phantomjs is missing. Please run 'npm install gulp-mocha-phantomjs --no-save' in src/Raven.Studio directory, and rerun the tests. " + e);
    }

    const mocha = mochaPhantomJs({
        reporter: 'spec', //use json for debugging,
        suppressStdout: false,
        suppressStderr: false
    });

    return gulp.src(PATHS.test.html).pipe(mocha);
}

function watch() {
    gulp.watch(PATHS.tsSource, z_compile_app_changed);
    gulp.watch(PATHS.test.tsSource, z_compile_test);
    gulp.watch(PATHS.lessSourcesToWatch, less);
}

const compile_chain = gulp.series(
    gulp.parallel(z_parse_handlers, z_parse_configuration, z_generate_typings),
    less,
    z_compile_app
);

const test_chain = gulp.series(compile_chain, z_compile_test, z_generate_test_list, z_mocha_tests);

const watch_chain = gulp.series(compile_chain, watch);

const restore_chain = gulp.series(z_bower, z_typings);

const restore_compile_chain = gulp.series(restore_chain, compile_chain);

const release_chain = gulp.series(
    z_clean, 
    restore_compile_chain, 
    gulp.parallel(
        z_release_libs,
        z_release_copy_version,
        z_release_favicon,
        z_release_ace_workers,
        z_release_images,
        z_release_html,
        z_release_css_common,
        z_release_theme_css,
        z_release_fonts,
        z_release_durandal
    ), 
    z_release_package);

exports.less = less;
exports.compile = compile_chain;
exports.restore = restore_chain;
exports.restore_compile = restore_compile_chain;
exports.test = test_chain;
exports.watch = watch_chain;
exports.release = release_chain;
exports.build = restore_compile_chain;
exports.default = restore_compile_chain;
exports.clean = z_clean;
